from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from apscheduler import AsyncScheduler
from apscheduler.triggers.cron import CronTrigger

from app.database import close_db, init_db
from app.jobs.fetch_calendars import sync_all_calendars
from app.jobs.fetch_stock import sync_all_stocks

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from fastapi import FastAPI

log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[dict]:
    await init_db()

    async with AsyncScheduler() as scheduler:
        await scheduler.add_schedule(
            sync_all_stocks, CronTrigger(minute=0), id="sync_stocks"
        )
        await scheduler.add_schedule(
            sync_all_calendars, CronTrigger(minute=0), id="sync_calendars"
        )

        # Run initial sync in background so server starts immediately
        await scheduler.add_job(sync_all_stocks)
        await scheduler.add_job(sync_all_calendars)

        task = asyncio.create_task(scheduler.run_until_stopped())
        app.state.scheduler = scheduler
        yield {"scheduler": scheduler}
        task.cancel()

    await close_db()
