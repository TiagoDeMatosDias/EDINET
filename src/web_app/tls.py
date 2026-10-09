"""TLS certificate provisioning for the local web workstation.

The server always terminates TLS. On startup it reuses the first usable
certificate/key pair found in the data folder's ``certs/``, and when no
pair is present it generates a fresh self-signed pair there so later
startups reuse the same certificate.
"""

from __future__ import annotations

import datetime
import ipaddress
import logging
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from src.paths import certs_dir
from src.web_app.security import SecurityConfigurationError

logger = logging.getLogger(__name__)

#: Candidate (certificate, key) filename pairs, tried in order. Names follow
#: common conventions so dropped-in files such as Let's Encrypt
#: ``fullchain.pem``/``privkey.pem`` or ``cert.pem``/``key.pem`` are picked up.
CERTIFICATE_PAIRS: tuple[tuple[str, str], ...] = (
    ("cert.pem", "key.pem"),
    ("fullchain.pem", "privkey.pem"),
    ("tls.crt", "tls.key"),
    ("server.crt", "server.key"),
)

GENERATED_CERT_NAME = "cert.pem"
GENERATED_KEY_NAME = "key.pem"

_SELF_SIGNED_VALIDITY_DAYS = 3650  # ten years; browsers must be told to trust it anyway
_SELF_SIGNED_KEY_SIZE_BITS = 2048


def default_cert_dir() -> Path:
    """Return the certificate directory used when none is supplied: ``data/certs``."""
    return certs_dir()


def _find_certificate_pair(cert_dir: Path) -> tuple[Path, Path] | None:
    """Return the first existing (certificate, key) pair in ``cert_dir``."""
    for cert_name, key_name in CERTIFICATE_PAIRS:
        cert_path = cert_dir / cert_name
        key_path = cert_dir / key_name
        if cert_path.is_file() and key_path.is_file():
            return cert_path, key_path
    return None


def _pair_is_consistent(cert_path: Path, key_path: Path) -> bool:
    """Return whether the key file matches the public key in the certificate."""
    try:
        certificate = x509.load_pem_x509_certificate(cert_path.read_bytes())
        private_key = serialization.load_pem_private_key(
            key_path.read_bytes(), password=None
        )
    except (ValueError, OSError):
        return False
    return (
        certificate.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        == private_key.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
    )


def _subject_alternative_names(host: str) -> list[x509.GeneralName]:
    """Build SAN entries covering loopback plus the configured bind host."""
    names: list[x509.GeneralName] = [x509.DNSName("localhost")]
    ip_names = {ipaddress.ip_address("127.0.0.1"), ipaddress.ip_address("::1")}
    normalized_host = host.strip().strip("[]")
    try:
        address = ipaddress.ip_address(normalized_host)
    except ValueError:
        if normalized_host and normalized_host.casefold() not in {
            "localhost",
            "0.0.0.0",
            "::",
        }:
            names.append(x509.DNSName(normalized_host))
    else:
        if not address.is_unspecified:
            ip_names.add(address)
    names.extend(
        x509.IPAddress(address) for address in sorted(ip_names, key=str)
    )
    return names


def _write_pair(
    cert_path: Path,
    key_path: Path,
    directory: Path,
    host: str,
) -> None:
    """Generate and persist a self-signed certificate and private key."""
    key = rsa.generate_private_key(
        public_exponent=65_537,
        key_size=_SELF_SIGNED_KEY_SIZE_BITS,
    )
    name = x509.Name(
        [
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Shade Research"),
            x509.NameAttribute(NameOID.COMMON_NAME, "Shade Research Workstation"),
        ]
    )
    now = datetime.datetime.now(datetime.timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(
            now + datetime.timedelta(days=_SELF_SIGNED_VALIDITY_DAYS)
        )
        .add_extension(
            x509.SubjectAlternativeName(_subject_alternative_names(host)),
            critical=False,
        )
        .add_extension(
            x509.BasicConstraints(ca=False, path_length=None),
            critical=True,
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_encipherment=True,
                content_commitment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=None,
                decipher_only=None,
            ),
            critical=True,
        )
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    directory.mkdir(parents=True, exist_ok=True)
    key_bytes = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    )
    cert_bytes = certificate.public_bytes(serialization.Encoding.PEM)
    key_path.write_bytes(key_bytes)
    try:
        key_path.chmod(0o600)
    except OSError:  # Windows permissions are advisory here
        pass
    cert_path.write_bytes(cert_bytes)


def provision_tls(
    *,
    cert_dir: Path | None = None,
    host: str = "127.0.0.1",
) -> tuple[Path, Path]:
    """Return the (certificate, key) paths the server should serve TLS with.

    Reuses the first usable pair found in the certificate directory. When no
    pair exists a self-signed pair is generated there, so HTTPS works out of
    the box and subsequent startups reuse the same certificate.

    Raises:
        SecurityConfigurationError: A certificate/key pair exists but is
            unreadable or does not match; user-provided material is never
            silently overwritten.
    """
    directory = Path(cert_dir) if cert_dir is not None else default_cert_dir()
    existing = _find_certificate_pair(directory)
    if existing is not None:
        cert_path, key_path = existing
        if not _pair_is_consistent(cert_path, key_path):
            raise SecurityConfigurationError(
                f"TLS certificate {cert_path} and key {key_path} in "
                f"{directory} are unreadable or do not match. Fix or remove "
                "them, or place a certificate under one of the supported "
                f"pairs ({', '.join('/'.join(pair) for pair in CERTIFICATE_PAIRS)})."
            )
        logger.info("Serving HTTPS with certificate %s", cert_path)
        return cert_path, key_path

    cert_path = directory / GENERATED_CERT_NAME
    key_path = directory / GENERATED_KEY_NAME
    _write_pair(cert_path, key_path, directory, host)
    logger.warning(
        "No TLS certificate found in %s; generated self-signed pair %s / %s. "
        "Browsers will warn until the certificate is trusted on this machine.",
        directory,
        cert_path,
        key_path,
    )
    return cert_path, key_path
