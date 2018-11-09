/* -------------------------------------------------------------------------
 *
 * pg_fact_loader.c
 *		Cloned code from worker_spi and modified to accomplish our goals.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/backendid.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* these headers are used by this particular worker's code */
#include "access/xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "tcop/utility.h"

/* includes for ticker */
#include "commands/dbcommands.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pg_fact_loader_worker);

void		pg_fact_loader_main(Datum) pg_attribute_noreturn();

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

/* GUC variables */
static int  ticker_naptime = 60;

/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void
pg_fact_loader_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * Signal handler for SIGHUP
 *		Set a flag to tell the main loop to reread the config file, and set
 *		our latch to wake it up.
 */
static void
pg_fact_loader_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

void
pg_fact_loader_main(Datum main_arg)
{
	Oid db_oid_main = DatumGetObjectId(main_arg);

	StringInfoData buf;

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, pg_fact_loader_sighup);
	pqsignal(SIGTERM, pg_fact_loader_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
#if PG_VERSION_NUM >= 110000 
	BackgroundWorkerInitializeConnectionByOid(db_oid_main, InvalidOid, 0);
#else
	BackgroundWorkerInitializeConnectionByOid(db_oid_main, InvalidOid);
#endif

	elog(LOG, "%s initialized",
		 MyBgworkerEntry->bgw_name);

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "SELECT fact_loader.worker();");

	/*
	 * Main loop: do this until the SIGTERM handler tells us to terminate
	 */
	while (!got_sigterm)
	{
		int			rc;

		/*
		 * Background workers mustn't call usleep() or any direct equivalent:
		 * instead, they may wait on their process latch, which sleeps as
		 * necessary, but is awakened if postmaster dies.  That way the
		 * background process goes away immediately in an emergency.
		 */
#if PG_VERSION_NUM >= 100000
        rc = WaitLatch(MyLatch,
                       WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                       ticker_naptime * 1000L,
                       PG_WAIT_EXTENSION);
#else
        rc = WaitLatch(MyLatch,
                       WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                       ticker_naptime * 1000L);
#endif		
		ResetLatch(MyLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		CHECK_FOR_INTERRUPTS();

		/*
		 * In case of a SIGHUP, just reload the configuration.
		 */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/*
		 * Start a transaction on which we can run queries.  Note that each
		 * StartTransactionCommand() call should be preceded by a
		 * SetCurrentStatementStartTimestamp() call, which sets both the time
		 * for the statement we're about the run, and also the transaction
		 * start time.  Also, each other query sent to SPI should probably be
		 * preceded by SetCurrentStatementStartTimestamp(), so that statement
		 * start time is always up to date.
		 *
		 * The SPI_connect() call lets us run queries through the SPI manager,
		 * and the PushActiveSnapshot() call creates an "active" snapshot
		 * which is necessary for queries to have MVCC data to work on.
		 *
		 * The pgstat_report_activity() call makes our activity visible
		 * through the pgstat views.
		 */
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		/*
		 * We always want the worker() function to run in at least repeatable read
		 * isolation level to always get the same snapshot with concurrent traffic
		 */
		XactIsoLevel = XACT_REPEATABLE_READ;
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());
		pgstat_report_activity(STATE_RUNNING, buf.data);

		/* We can now execute queries via SPI */
		SPI_execute(buf.data, false, 0);

		/*
		 * And finish our transaction.
		 */
		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
		pgstat_report_stat(false);
		pgstat_report_activity(STATE_IDLE, NULL);
	}

	proc_exit(1);
}

/*
 * Dynamically launch an SPI worker.
 */
Datum
pg_fact_loader_worker(PG_FUNCTION_ARGS)
{
	Oid         db_oid = PG_GETARG_OID(0);
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t		pid;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	sprintf(worker.bgw_library_name, "pg_fact_loader");
	sprintf(worker.bgw_function_name, "pg_fact_loader_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_fact_loader worker");
#if PG_VERSION_NUM >= 110000 
	snprintf(worker.bgw_type, BGW_MAXLEN, "pglogical_ticker");
#endif
	worker.bgw_main_arg = ObjectIdGetDatum(db_oid);
	/* set bgw_notify_pid so that we can use WaitForBackgroundWorkerStartup */
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		PG_RETURN_NULL();

	status = WaitForBackgroundWorkerStartup(handle, &pid);

	if (status == BGWH_STOPPED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start background process"),
				 errhint("More details may be available in the server log.")));
	if (status == BGWH_POSTMASTER_DIED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("cannot start background processes without postmaster"),
				 errhint("Kill all remaining database processes and restart the database.")));
	Assert(status == BGWH_STARTED);

	PG_RETURN_INT32(pid);
}

