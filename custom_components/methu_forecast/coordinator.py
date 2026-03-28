"""DataUpdateCoordinator for HungaroMet (met.hu) Forecast."""
from __future__ import annotations

import logging
from datetime import timedelta

import aiohttp
import asyncio
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import DOMAIN
from .scraper import MetHuForecastData, Settlement, fetch_forecast

_LOGGER = logging.getLogger(__name__)


class MetHuForecastCoordinator(DataUpdateCoordinator[MetHuForecastData]):
    """Coordinator to manage fetching data from met.hu."""

    def __init__(
        self,
        hass: HomeAssistant,
        settlement: Settlement,
        scan_interval: int,
    ) -> None:
        """Initialize the coordinator."""
        self.settlement = settlement

        super().__init__(
            hass,
            _LOGGER,
            name=f"{DOMAIN}_{settlement.name}",
            update_interval=timedelta(minutes=scan_interval),
        )

    async def _async_update_data(self) -> MetHuForecastData:
        """Fetch data from met.hu."""
        # Note: asyncio.TimeoutError and aiohttp.ClientError are already
        # handled by the data update coordinator.
        session = async_get_clientsession(self.hass)
        try:
            data = await fetch_forecast(session, self.settlement)
        except aiohttp.ClientError as exc:
            raise
        except asyncio.TimeoutError as exc:
            raise
        except Exception as exc:
            raise UpdateFailed(
                f"Error communicating with met.hu for '{self.settlement.name}': {exc}"
            ) from exc

        if not data.settlement_found:
            _LOGGER.warning(
                "met.hu returned no forecast data for '%s' (kod=%s).",
                self.settlement.name,
                self.settlement.kod,
            )

        return data
