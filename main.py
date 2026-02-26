from __future__ import annotations

import uvicorn
from fastapi import FastAPI

from database import close_db, init_db
from jobs.scheduler import lifespan
from routes.admin import router as admin_router
from routes.calendars import router as calendars_router
from routes.stocks import router as stocks_router

app = FastAPI(title="yaefinance", lifespan=lifespan)
app.include_router(stocks_router)
app.include_router(calendars_router)
app.include_router(admin_router)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8011, reload=True)
