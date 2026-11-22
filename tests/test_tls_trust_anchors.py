import asyncio
import hashlib
import ssl
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from ipaddress import ip_address
from pathlib import Path
from unittest.mock import patch

from aiohttp import web
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from grpclib.const import Cardinality, Handler
from grpclib.server import Server

from PasarGuardNodeBridge.common import service_pb2 as service
from PasarGuardNodeBridge.controller import NodeAPIError
from PasarGuardNodeBridge.grpclib import Node as GrpcNode
from PasarGuardNodeBridge.rest import Node as RestNode
from tests.test_tls import KEY, certificate

try:
    import uvloop
except ImportError:
    uvloop = None

TRANSPORTS = ("rest", "maintenance", "grpc")
INTERPRETER_DEFAULTS = ("as shipped", "without partial chain")


def issued_by_absent_ca(directory):
    ca_key = ec.generate_private_key(ec.SECP256R1())
    key = ec.generate_private_key(ec.SECP256R1())
    now = datetime.now(UTC)
    leaf = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1")]))
        .issuer_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Node CA")]))
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ip_address("127.0.0.1"))]), False)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), False)
        .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), False)
        .sign(ca_key, hashes.SHA256())
    )
    pem = leaf.public_bytes(serialization.Encoding.PEM).decode()
    cert_path, key_path = Path(directory) / "leaf.pem", Path(directory) / "leaf-key.pem"
    cert_path.write_text(pem)
    key_path.write_bytes(
        key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    )
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(cert_path, key_path)
    ctx.set_alpn_protocols(["h2", "http/1.1"])
    return ctx, pem, hashlib.sha256(leaf.public_bytes(serialization.Encoding.DER)).hexdigest()


class TrustAnchorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        loop = asyncio.get_running_loop()

        def expected_handshake_reset(loop, context):
            if isinstance(context.get("exception"), ConnectionResetError):
                return
            loop.default_exception_handler(context)

        loop.set_exception_handler(expected_handshake_reset)

    async def connect(self, transport, mode, defaults, bundle, peer, calls):
        if transport == "grpc":

            class Receiver:
                async def receive(inner, stream):
                    await stream.recv_message()
                    calls.append(transport)
                    await stream.send_message(service.BaseInfoResponse())

                def __mapping__(inner):
                    return {
                        "/service.NodeService/GetBaseInfo": Handler(
                            inner.receive, Cardinality.UNARY_UNARY, service.Empty, service.BaseInfoResponse
                        )
                    }

            server = Server([Receiver()])
            await server.start("127.0.0.1", 0, ssl=peer)
            port = server._server.sockets[0].getsockname()[1]
        else:

            async def receive(request):
                calls.append(transport)
                return web.Response(body=b"ok")

            app = web.Application()
            app.router.add_get("/", receive)
            server = web.AppRunner(app)
            await server.setup()
            site = web.TCPSite(server, "127.0.0.1", 0, ssl_context=peer)
            await site.start()
            port = site._server.sockets[0].getsockname()[1]
        original = ssl.create_default_context

        def interpreter_context(*args, **kwargs):
            ctx = original(*args, **kwargs)
            if defaults == "without partial chain":
                ctx.verify_flags &= ~ssl.VERIFY_X509_PARTIAL_CHAIN
            if mode == "system":
                ctx.load_verify_locations(cadata=bundle)
            return ctx

        with patch("ssl.create_default_context", side_effect=interpreter_context):
            node_class = GrpcNode if transport == "grpc" else RestNode
            node = node_class("127.0.0.1", port, port, KEY, server_ca=bundle if mode == "pem" else None)
        try:
            if transport == "grpc":
                await node._handle_grpc_request(node._client.GetBaseInfo, service.Empty())
            elif transport == "maintenance":
                await node._make_json_request("GET", "/")
            else:
                await node._make_request("GET", "/", 3)
            return node.get_tls_status()["maintenance" if transport == "maintenance" else "service"]
        finally:
            if transport == "grpc":
                node.channel.close()
                server.close()
                await server.wait_closed()
            else:
                await node._client.close()
                await server.cleanup()
            await node._json_client.close()

    async def test_every_same_name_self_signed_certificate_in_the_bundle_is_trusted(self):
        with tempfile.TemporaryDirectory() as first_dir, tempfile.TemporaryDirectory() as next_dir:
            first_ctx, first_pem, first_pin = certificate(first_dir)
            next_ctx, next_pem, next_pin = certificate(next_dir)
            for defaults in INTERPRETER_DEFAULTS:
                for mode in ("pem", "system"):
                    for transport in TRANSPORTS:
                        for peer, pin in ((first_ctx, first_pin), (next_ctx, next_pin)):
                            with self.subTest(defaults=defaults, mode=mode, transport=transport, peer=pin[:12]):
                                calls = []
                                status = await self.connect(
                                    transport, mode, defaults, first_pem + next_pem, peer, calls
                                )
                                self.assertEqual(status["fingerprint"], pin)
                                self.assertIsNone(status["connection_error"])
                                self.assertEqual(calls, [transport])

    async def test_a_same_name_certificate_missing_from_the_bundle_gets_no_credentials(self):
        with (
            tempfile.TemporaryDirectory() as first_dir,
            tempfile.TemporaryDirectory() as other_dir,
            tempfile.TemporaryDirectory() as stranger_dir,
        ):
            _, first_pem, _ = certificate(first_dir)
            _, other_pem, _ = certificate(other_dir)
            stranger_ctx, _, _ = certificate(stranger_dir)
            for defaults in INTERPRETER_DEFAULTS:
                for mode in ("pem", "system"):
                    for transport in TRANSPORTS:
                        for bundle in (first_pem, first_pem + other_pem):
                            with self.subTest(defaults=defaults, mode=mode, transport=transport, size=len(bundle)):
                                calls = []
                                with self.assertRaisesRegex(NodeAPIError, "CERTIFICATE_VERIFY_FAILED"):
                                    await self.connect(transport, mode, defaults, bundle, stranger_ctx, calls)
                                self.assertEqual(calls, [])

    async def test_a_bundled_leaf_whose_issuer_is_absent_is_trusted(self):
        with tempfile.TemporaryDirectory() as directory:
            peer, pem, pin = issued_by_absent_ca(directory)
            for defaults in INTERPRETER_DEFAULTS:
                for mode in ("pem", "system"):
                    for transport in TRANSPORTS:
                        with self.subTest(defaults=defaults, mode=mode, transport=transport):
                            calls = []
                            status = await self.connect(transport, mode, defaults, pem, peer, calls)
                            self.assertEqual(status["fingerprint"], pin)
                            self.assertEqual(calls, [transport])


@unittest.skipIf(uvloop is None, "uvloop is unavailable on this platform")
class UVLoopTrustAnchorTests(TrustAnchorTests):
    @staticmethod
    def loop_factory():
        return uvloop.new_event_loop()

    def _setupAsyncioRunner(self):
        self._asyncioRunner = asyncio.Runner(debug=True, loop_factory=self.loop_factory)
