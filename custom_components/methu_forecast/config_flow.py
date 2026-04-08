"""Config flow for HungaroMet (met.hu) Forecast integration."""
from __future__ import annotations

import logging

import aiohttp
import voluptuous as vol
from homeassistant import config_entries
from homeassistant.core import callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import (
    CONF_KOD,
    CONF_LAT,
    CONF_LON,
    CONF_SCAN_INTERVAL,
    CONF_SETTLEMENT,
    CONF_SETTLEMENT_NAME,
    DEFAULT_SCAN_INTERVAL,
    DOMAIN,
    MIN_SCAN_INTERVAL,
)
from .scraper import Settlement, lookup_settlement

_LOGGER = logging.getLogger(__name__)

# Schema for step 1: user types a settlement name
STEP_USER_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_SETTLEMENT): str,
        vol.Optional(CONF_SCAN_INTERVAL, default=DEFAULT_SCAN_INTERVAL): vol.All(
            vol.Coerce(int), vol.Range(min=MIN_SCAN_INTERVAL, max=1440)
        ),
    }
)

# Schema for the manual fallback step (if autocomplete fails)
STEP_MANUAL_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_KOD): str,
        vol.Required(CONF_LAT): vol.Coerce(float),
        vol.Required(CONF_LON): vol.Coerce(float),
    }
)

OPTIONS_SCHEMA = vol.Schema(
    {
        vol.Optional(CONF_SCAN_INTERVAL, default=DEFAULT_SCAN_INTERVAL): vol.All(
            vol.Coerce(int), vol.Range(min=MIN_SCAN_INTERVAL, max=1440)
        ),
    }
)


class MetHuForecastConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for HungaroMet Forecast."""

    VERSION = 1

    def __init__(self) -> None:
        """Initialise flow state."""
        self._settlement_input: str = ""
        self._scan_interval: int = DEFAULT_SCAN_INTERVAL

    async def async_step_user(self, user_input=None):
        """
        Step 1: user enters settlement name.

        We try to resolve it via the met.hu autocomplete endpoint.
        On success → create entry.
        On autocomplete failure → offer manual kod/lat/lon entry.
        """
        errors: dict[str, str] = {}

        if user_input is not None:
            self._settlement_input = user_input[CONF_SETTLEMENT].strip()
            self._scan_interval = user_input.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL)

            # Prevent duplicates
            await self.async_set_unique_id(self._settlement_input.lower())
            self._abort_if_unique_id_configured()

            session = async_get_clientsession(self.hass)

            try:
                settlement = await lookup_settlement(session, self._settlement_input)
            except aiohttp.ClientConnectorError:
                errors["base"] = "cannot_connect"
                settlement = None
            except Exception:  # pylint: disable=broad-except
                _LOGGER.exception(
                    "Unexpected error looking up settlement '%s'",
                    self._settlement_input,
                )
                errors["base"] = "unknown"
                settlement = None

            if not errors and settlement is None:
                # Autocomplete returned nothing → fall back to manual entry
                return await self.async_step_manual()

            if not errors and settlement is not None:
                return self._create_entry(settlement)

        return self.async_show_form(
            step_id="user",
            data_schema=STEP_USER_SCHEMA,
            errors=errors,
            description_placeholders={
                "example": "Budapest, Pécs, Siófok, Várpalota …",
            },
        )

    async def async_step_manual(self, user_input=None):
        """
        Step 2 (fallback): if the autocomplete endpoint could not be reached
        or returned no results, let the user enter the kod/lat/lon manually.

        These can be found in the browser Network tab when selecting a
        settlement on https://www.met.hu/idojaras/elorejelzes/magyarorszagi_telepulesek/
        """
        errors: dict[str, str] = {}

        if user_input is not None:
            settlement = Settlement(
                name=self._settlement_input,
                kod=user_input[CONF_KOD].strip(),
                lat=float(user_input[CONF_LAT]),
                lon=float(user_input[CONF_LON]),
            )
            return self._create_entry(settlement)

        return self.async_show_form(
            step_id="manual",
            data_schema=STEP_MANUAL_SCHEMA,
            errors=errors,
            description_placeholders={
                "settlement": self._settlement_input,
            },
        )

    def _create_entry(self, settlement: Settlement) -> config_entries.FlowResult:
        """Persist the entry."""
        return self.async_create_entry(
            title=f"met.hu: {settlement.name}",
            data={
                CONF_SETTLEMENT: self._settlement_input,
                CONF_SETTLEMENT_NAME: settlement.name,
                CONF_KOD: settlement.kod,
                CONF_LAT: settlement.lat,
                CONF_LON: settlement.lon,
                CONF_SCAN_INTERVAL: self._scan_interval,
            },
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry):
        """Return the options flow handler."""
        return MetHuForecastOptionsFlow()


class MetHuForecastOptionsFlow(config_entries.OptionsFlow):
    """Handle options (update interval) for HungaroMet Forecast."""

    async def async_step_init(self, user_input=None):
        """Manage options."""
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)

        current_interval = self.config_entry.options.get(
            CONF_SCAN_INTERVAL,
            self.config_entry.data.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL),
        )

        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema(
                {
                    vol.Optional(CONF_SCAN_INTERVAL, default=current_interval): vol.All(
                        vol.Coerce(int), vol.Range(min=MIN_SCAN_INTERVAL, max=1440)
                    ),
                }
            ),
        )
