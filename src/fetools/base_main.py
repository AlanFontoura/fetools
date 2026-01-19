import sys
import logging
import argparse
import getpass
import json
import requests
import pandas as pd
from tqdm import tqdm
from multiprocess import Pool
from typing import Union, Optional, Any
from functools import partial
from fetools.d1g1tparser import ChartTableFormatter
from fetools.exceptions import NoResponseError


from drf_client.connection import (  # type: ignore[import-untyped]
    Api as RestApi,
    DEFAULT_HEADERS,
    RestResource,
)
from drf_client.exceptions import HttpClientError  # type: ignore[import-untyped]

LOG = logging.getLogger(__name__)


class D1g1tRestResource(RestResource):
    def post(self, data=None, **kwargs):
        """
        Overwrite RestResource 'post' method to handle
         d1g1t 202 'waiting' response status
        """
        if data:
            payload = json.dumps(data)
        else:
            payload = None
        url = self.url()
        headrs = self._get_headers()
        counter = 100
        resp = requests.post(url, data=payload, headers=headrs)
        while resp.status_code == 202 and counter > 0:
            resp = requests.post(url, data=payload, headers=headrs)
            counter -= 1
        return self._process_response(resp)


class D1g1tApi(RestApi):
    def _get_resource(self, **kwargs):
        """Overwrite to use custom D1g1tResource class"""
        return D1g1tRestResource(**kwargs)

    def d1g1t_login(self, password, username):
        assert "LOGIN" in self.options
        data = {"username": username, "password": password}
        url = "{0}/{1}".format(self.base_url, self.options["LOGIN"])

        payload = json.dumps(data)
        r = requests.post(url, data=payload, headers=DEFAULT_HEADERS)
        if r.status_code in [200, 201]:
            content = json.loads(r.content.decode())
            self.token = content["token"]
            self.username = username
            return True

        return False

    def refresh_login(self) -> bool:
        """
        token needs to be refreshed every 4hrs or so!
        :return:
        """
        api_auth = self.api.auth.login.refresh
        r = api_auth.post({"token": self.token})
        if r.status_code in [200, 201]:
            content = json.loads(r.content.decode())
            self.token = content["token"]
            return True
        return False


class BaseMain(object):
    parser: Optional[argparse.ArgumentParser] = None
    args: Optional[argparse.Namespace] = None
    api: Optional[D1g1tApi] = None
    options = {
        "DOMAIN": None,
        "API_PREFIX": "api/v1",
        "TOKEN_TYPE": "jwt",
        "TOKEN_FORMAT": "JWT {token}",
        "LOGIN": "auth/login/",
        "LOGOUT": "auth/logout/",
    }
    logging_level = logging.INFO

    def __init__(self):
        """
        Initialize Logging configuration
        Initialize argument parsing
        Process any extra arguments
        Only hard codes one required argument: --user
        Additional arguments can be configured by overwriting the add_extra_args() method
        Logging configuration can be changed by overwritting the config_logging() method
        """
        self.parser = argparse.ArgumentParser(description=__doc__)
        self.parser.add_argument(
            "-u",
            "--user",
            dest="username",
            type=str,
            required=False,
            help="Username used for login",
        )
        self.parser.add_argument(
            "--server",
            dest="server",
            type=str,
            required=False,
            help="Server Domain Name to use",
        )

        self.add_extra_args()

        self.args = self.parser.parse_args()
        self.config_logging()

    @staticmethod
    def _critical_exit(msg):
        LOG.error(msg)
        sys.exit(1)

    def main(self):
        """
        Main function to call to initiate execution.
        1. Get domain name and use to instantiate Api object
        2. Call before_login to allow for work before logging in
        3. Logging into the server
        4. Call after_loging to do actual work with server data
        """
        self.domain = self.get_domain()
        self.options["DOMAIN"] = self.domain
        self.api = D1g1tApi(self.options)
        self.before_login()
        ok = self.login()
        if ok:
            self.after_login()
        else:
            raise HttpClientError("Your login attempt was unseccessful!")

    # Following functions can be overwritten if needed
    # ================================================

    def config_logging(self):
        """
        Overwrite to change the way the logging package is configured
        :return: Nothing
        """
        logging.basicConfig(
            level=self.logging_level,
            format="[%(asctime)-15s] %(levelname)-6s %(message)s",
            datefmt="%d/%b/%Y %H:%M:%S",
        )

    def add_extra_args(self):
        """
        Overwrite to change the way extra arguments are added to the args resp_prsr
        :return: Nothing
        """
        pass

    def get_domain(self) -> str:
        """
        Figure out server domain URL based on --server and --customer args
        """
        assert self.args is not None, "Arguments not initialized"
        server = str(self.args.server)
        if "https://" not in server:
            return f"https://{server}"
        return server

    def login(self) -> bool:
        """
        Get password from user and login
        """
        assert self.api is not None, "API not initialized"
        assert self.args is not None, "Arguments not initialized"
        password = getpass.getpass()
        ok = self.api.d1g1t_login(
            username=self.args.username, password=password
        )
        if ok:
            LOG.info("Welcome {0}".format(self.args.username))
        return bool(ok)

    def refresh_login(self) -> None:
        """
        token needs to be refreshed every 4hrs or so!
        :return:
        """
        assert self.api is not None, "API not initialized"
        api_auth = self.api.auth.login.refresh
        tok = api_auth._store["token"]
        r = api_auth.post({"token": tok})
        api_auth._store["token"] = r["token"]
        LOG.info("Token refreshed")

    def before_login(self):
        """
        Overwrite to do work after parsing, but before logging in to the server
        This is a good place to do additional custom argument checks
        :return: Nothing
        """
        pass

    def after_login(self):
        """
        This function MUST be overwritten to do actual work after logging into the Server
        :return: Nothing
        """
        LOG.warning("No actual work done")


class ReportGeneric(BaseMain):
    def __init__(self):
        BaseMain.__init__(self)

    def get_calculation(self, calc_type: str, payload: dict) -> pd.DataFrame:
        assert self.api is not None, "API not initialized"
        calc_call = self.api.calc(calc_type)
        response = calc_call.post(data=payload)
        if not response:
            raise NoResponseError("Request returned no result!")
        parser = ChartTableFormatter(response, payload)
        result = parser.parse_data()
        return result

    def get_data(
        self, data_type, extra_params, to_frame=True
    ) -> Union[dict, pd.DataFrame]:
        assert self.api is not None, "API not initialized"
        api_call = self.api.data
        api_call._store["base_url"] += f"{data_type}/"
        response = api_call.get(extra=extra_params)
        if response:
            if to_frame:
                return pd.DataFrame(response["results"])  # type: ignore[no-any-return]
            else:
                return dict(response["results"])
        else:
            raise NoResponseError

    def get_large_data(self, data_type, batch_size=1000):
        assert self.api is not None, "API not initialized"
        api_call = self.api.data
        api_call._store["base_url"] += f"{data_type}/"
        try:
            response = api_call.get(extra=f"limit={1}")
            total_entries = response["count"]
            print(f"Total number of entries: {total_entries}")
        except:
            raise NoResponseError("Request returned no result!")

        total_batches = (total_entries // batch_size) + 1
        extras = [
            f"limit={batch_size}&offset={i * batch_size}"
            for i in range(total_batches)
        ]

        worker = partial(self.get_data, data_type)

        with Pool() as pool:
            results = list(
                tqdm(pool.imap(worker, extras), total=total_batches)
            )

        final_df = pd.concat(results, ignore_index=True)
        return final_df
