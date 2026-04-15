---
name: d1g1t-scripts

description: "When the user asks you to write a Python script that interacts with the d1g1t platform API, follow these instructions."
---
## Dependencies

The script requires [django-rest-framework-client](https://github.com/dkarchmer/django-rest-framework-client) (`pip install "django-rest-framework-client>=0.4.1"`).

## Script Structure

Every d1g1t script follows a class hierarchy. You MUST include the full base classes (`D1g1tMain`, `D1g1tMainIniToken`) in every script — they are not importable from a shared package. The user's script subclasses `D1g1tMainIniToken` and implements `after_login()`.

### Required Base Code

Always include these imports and base classes at the top of every script:

```python
"""<Script description here>."""
import argparse
import configparser
import getpass
import logging
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

from drf_client.helpers.base_main import BaseMain
from drf_client.exceptions import HttpClientError, HttpServerError

logger = logging.getLogger(__name__)


def error_and_exit(msg, *args):
    """Log error and exit."""
    if hasattr(msg, "content"):
        logger.error(msg.content)
    logger.error(msg, *args)
    sys.exit(1)


def refresh_api_token(api) -> bool:
    """Refresh token to ensure it won't expire."""
    resource = api.auth.login.refresh
    logger.debug(f"==> Refreshing token using {resource.url()}")
    try:
        tokens = resource.post({"token": api.token})
        api.token = tokens.get("token")
        return True
    except HttpClientError as err:
        msg = f"Error when calling {resource.url()}: {err.content}"
        error_and_exit(msg)
    return False


class D1g1tMain(BaseMain):
    """Shared BaseMain configuration for d1g1t."""

    logging_level = logging.INFO
    _log_filename = "out.log"

    def get_options(self):
        """Set USE_DASHES and API_PREFIX for d1g1t."""
        options = super().get_options()
        options["USE_DASHES"] = True
        options["API_PREFIX"] = "api"
        return options

    def config_logging(self):
        """Configure logging with console and file handlers."""
        root_logger = logging.getLogger()
        root_logger.setLevel(self.logging_level)
        formatter = logging.Formatter("[%(asctime)-15s] %(levelname)-6s %(message)s")
        ch = logging.StreamHandler()
        ch.setLevel(self.logging_level)
        ch.setFormatter(formatter)
        fh = logging.FileHandler(self._log_filename, mode="w")
        fh.setLevel(self.logging_level)
        fh.setFormatter(formatter)
        root_logger.addHandler(ch)
        root_logger.addHandler(fh)

    def refresh_token(self) -> bool:
        """Refresh token to ensure it won't expire."""
        return refresh_api_token(self.api)


class D1g1tMainIniToken(D1g1tMain):
    """
    Base class supporting multiple login methods:
    1. Env var DRF_CLIENT_AUTH_TOKEN -> use as Bearer token
    2. --profile with .ini file containing TOKEN -> use as Bearer token
    3. --user with --server -> prompt for password, use JWT auth
    """

    config: configparser.ConfigParser | None = None
    ini_file = Path(__file__).parent.resolve() / Path(".ini")

    def __init__(self):
        """Set up argument parser and read .ini if profile given."""
        self.parser = argparse.ArgumentParser(description=__doc__)
        self.parser.add_argument(
            "-u", "--user", dest="username", type=str, required=False,
            help="Username used for login",
        )
        self.parser.add_argument(
            "--profile", dest="profile", required=False, type=str,
            help="INI Profile (section name)",
        )
        self.parser.add_argument(
            "--server", dest="server", type=str, required=False,
            help="Server Domain Name",
        )
        self.add_extra_args()
        self.args = self.parser.parse_args()
        self.config_logging()
        self.domain = ""
        self.config = configparser.ConfigParser()
        if self.args.profile:
            logger.info(f"Reading INI File: {self.ini_file} (Profile {self.args.profile})")
            self.config.read(str(self.ini_file))

    @property
    def profile_token(self) -> str:
        if not self.args.profile:
            return ""
        if self.args.profile.upper() in self.config:
            return self.config[self.args.profile.upper()].get("TOKEN", "")
        return ""

    @property
    def env_token(self) -> str:
        return os.getenv("DRF_CLIENT_AUTH_TOKEN", "")

    @property
    def env_server_domain(self) -> str:
        return os.getenv("DRF_CLIENT_SERVER", "")

    def refresh_token(self) -> bool:
        if self.env_token or (self.args.profile and self.profile_token):
            return True
        return refresh_api_token(self.api)

    def get_options(self):
        options = super().get_options()
        if self.env_token or (self.args.profile and self.profile_token):
            options["TOKEN_FORMAT"] = "Bearer {token}"
            options["TOKEN_TYPE"] = "bearer"
        return options

    def get_domain(self) -> str:
        server = self.args.server
        if not server and self.env_server_domain:
            server = self.env_server_domain
        if not server and self.args.profile and self.args.profile.upper() in self.config:
            server = self.config[self.args.profile.upper()].get("SERVER")
        if not server:
            error_and_exit("--server or .ini with SERVER is required")
        if not urlparse(server).scheme:
            return f"https://{server}"
        return server

    def login(self) -> bool:
        if self.env_token or (self.args.profile and self.profile_token):
            token = self.env_token or self.profile_token
            if not token:
                error_and_exit("Token not found in .ini file or environment.")
                return False
            self.api.set_token(token)
            logger.info("Using permanent token from Env Variable or .INI")
            return True
        else:
            if not self.args.username:
                error_and_exit("--user is required when not using a token")
                return False
            password = getpass.getpass()
            ok = self.api.login(username=self.args.username, password=password)
            if ok:
                logger.info(f"Welcome {self.args.username}.")
            return ok
```

## Writing the User's Script

After the base classes, write the user's actual script class:

```python
class MyScript(D1g1tMainIniToken):

    def add_extra_args(self):
        """Add script-specific CLI arguments here."""
        # Example:
        # self.parser.add_argument("--output", type=str, help="Output file path")
        pass

    def after_login(self):
        """Main script logic — runs after successful login."""
        # Your code here. Use self.api to make API calls.
        pass


if __name__ == "__main__":
    work = MyScript()
    work.main()
```

## Critical Rules

### d1g1t API Configuration

- **`USE_DASHES = True`** is already set in `D1g1tMain`. d1g1t URLs use dashes (`/api/v1/firm-portfolios/`), but Python code uses underscores (`self.api.v1.firm_portfolios`). The client converts automatically.
- **`API_PREFIX = "api"`** is already set. You MUST always include the version in the attribute chain: `self.api.v1.resource` or `self.api.v3.resource`. Never assume all endpoints are v1.

### Making API Calls

```python
# GET a list
resp = self.api.v1.data.instruments.get()

# GET with query params
resp = self.api.v1.data.instruments.get(extra='?page_size=100&status=active')

# GET a single resource by ID
resp = self.api.v1.data.instruments(123).get()

# POST
resp = self.api.v1.some.resource.post(data={'key': 'value'})

# PATCH (partial update)
resp = self.api.v1.some.resource(123).patch(data={'key': 'new_value'})

# PUT (full update)
resp = self.api.v1.some.resource(123).put(data={'key': 'value'})

# DELETE
resp = self.api.v1.some.resource(123).delete()
```

### Paginated Results

d1g1t list endpoints return paginated responses:

```python
resp = self.api.v1.data.instruments.get()
for item in resp['results']:
    logger.info(item['name'])
# resp['next'] contains the URL for the next page, if any
```

### Error Handling

```python
from drf_client.exceptions import HttpClientError, HttpServerError, HttpNotFoundError

try:
    resp = self.api.v1.data.instruments(999).get()
except HttpNotFoundError:
    logger.error("Not found")
except HttpClientError as err:
    logger.error(f"Client error: {err.content}")
except HttpServerError as err:
    logger.error(f"Server error: {err.content}")
```

### Token Refresh for Long-Running Scripts

For scripts that run for a long time, call `self.refresh_token()` periodically. This is a no-op for Bearer tokens (env var / INI) and refreshes the JWT for password-based auth.

### Adding Custom CLI Arguments

Override `add_extra_args()` to add script-specific arguments:

```python
def add_extra_args(self):
    self.parser.add_argument("--output", type=str, required=True, help="Output CSV path")
    self.parser.add_argument("--limit", type=int, default=100, help="Max results")

def after_login(self):
    # Access with self.args.output, self.args.limit
    pass
```

### Running the Script

```bash
# With username/password (will prompt for password):
python my_script.py --server api-demo1.d1g1tdemo.com -u user@d1g1t.com

# With .ini profile:
python my_script.py --profile demo1

# With environment variables:
export DRF_CLIENT_AUTH_TOKEN="your-token"
export DRF_CLIENT_SERVER="api-demo1.d1g1tdemo.com"
python my_script.py
```