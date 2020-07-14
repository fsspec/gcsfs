from tornado.web import RequestHandler, Application
from tornado.ioloop import IOLoop
from aiogoogle import Aiogoogle
from aiogoogle.auth.utils import create_secret
from gcsfs.core import not_secret
import logging


logging.basicConfig()
EMAIL = "client email"
CLIENT_CREDS = {
    "scopes": ["https://www.googleapis.com/auth/devstorage.read_only"],
    "redirect_uri": "http://localhost:5000/callback/aiogoogle",
}
CLIENT_CREDS.update(**not_secret)
state = create_secret()  # Shouldn't be a global hardcoded variable.

LOCAL_HOST = "localhost"
LOCAL_PORT = "6666"
ADDRESS = LOCAL_HOST + ":" + LOCAL_HOST
URL_BASE = "http://" + ADDRESS

aiogoogle = Aiogoogle(client_creds=CLIENT_CREDS)


class AuthHandle(RequestHandler):
    def get(self):
        uri = aiogoogle.oauth2.authorization_url(
            client_creds=CLIENT_CREDS,
            state=state,
            access_type="offline",
            include_granted_scopes=True,
            login_hint=EMAIL,
            prompt="select_account",
        )
        # Step A
        return self.redirect(uri)


class CallBackHandle(RequestHandler):
    async def get(self):
        if self.args.get("error"):
            error = {
                "error": self.args.get("error"),
                "error_description": self.args.get("error_description"),
            }
            return self.write(error)
        elif self.args.get("code"):
            returned_state = self.args["state"][0]
            # Check state
            if returned_state != state:
                raise RuntimeError
            # Step D & E (D send grant code, E receive token info)
            full_user_creds = await aiogoogle.oauth2.build_user_creds(
                grant=self.args.get("code"), client_creds=CLIENT_CREDS
            )
            self.write(full_user_creds)
        else:
            # Should either receive a code or an error
            self.write("Something's probably wrong with your callback")


class MainHandler(RequestHandler):
    def get(self):
        self.write("OK")


if __name__ == "__main__":
    app = Application(
        [
            (r"/", MainHandler),
            ("/authorize", AuthHandle),
            ("/callback/aiogoogle", CallBackHandle),
        ]
    )
    app.listen(8898)
    IOLoop.current().start()
