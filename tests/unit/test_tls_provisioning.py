"""Unit tests for self-signed TLS certificate provisioning."""

from __future__ import annotations

import datetime
import ipaddress
import shutil
import stat

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import serialization

from src.web_app.security import SecurityConfigurationError
from src.web_app.tls import (
    GENERATED_CERT_NAME,
    GENERATED_KEY_NAME,
    default_cert_dir,
    provision_tls,
)


def _load_cert(path) -> x509.Certificate:
    return x509.load_pem_x509_certificate(path.read_bytes())


def _public_key_bytes(cert_path, key_path) -> tuple[bytes, bytes]:
    certificate = _load_cert(cert_path)
    private_key = serialization.load_pem_private_key(
        key_path.read_bytes(), password=None
    )
    return (
        certificate.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        ),
        private_key.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        ),
    )


def test_default_cert_dir_is_in_the_data_folder(tmp_path, monkeypatch):
    monkeypatch.setenv("EDINET_DATA_DIR", str(tmp_path / "elsewhere"))
    assert default_cert_dir() == tmp_path / "elsewhere" / "certs"


def test_provision_generates_self_signed_pair_when_folder_empty(tmp_path):
    cert_path, key_path = provision_tls(cert_dir=tmp_path, host="127.0.0.1")

    assert cert_path == tmp_path / GENERATED_CERT_NAME
    assert key_path == tmp_path / GENERATED_KEY_NAME
    certificate = _load_cert(cert_path)
    assert certificate.subject == certificate.issuer
    now = datetime.datetime.now(datetime.timezone.utc)
    valid_from = certificate.not_valid_before.replace(
        tzinfo=datetime.timezone.utc
    )
    valid_until = certificate.not_valid_after.replace(
        tzinfo=datetime.timezone.utc
    )
    assert valid_from <= now <= valid_until
    # Self-signed server certificate carries loopback SANs and a matching key.
    sans = certificate.extensions.get_extension_for_class(
        x509.SubjectAlternativeName
    ).value
    assert x509.DNSName("localhost") in sans
    assert x509.IPAddress(ipaddress.ip_address("127.0.0.1")) in sans
    cert_bytes, key_bytes = _public_key_bytes(cert_path, key_path)
    assert cert_bytes == key_bytes


def test_provision_reuses_existing_pair_without_regeneration(tmp_path):
    first = provision_tls(cert_dir=tmp_path, host="127.0.0.1")
    cert_mtime = (tmp_path / GENERATED_CERT_NAME).stat().st_mtime_ns
    key_mtime = (tmp_path / GENERATED_KEY_NAME).stat().st_mtime_ns

    second = provision_tls(cert_dir=tmp_path, host="127.0.0.1")

    assert second == first
    assert (tmp_path / GENERATED_CERT_NAME).stat().st_mtime_ns == cert_mtime
    assert (tmp_path / GENERATED_KEY_NAME).stat().st_mtime_ns == key_mtime


def test_provision_picks_up_letsencrypt_named_pair(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    generated = provision_tls(cert_dir=source, host="127.0.0.1")
    target = tmp_path / "target"
    target.mkdir()
    shutil.copy(generated[0], target / "fullchain.pem")
    shutil.copy(generated[1], target / "privkey.pem")

    cert_path, key_path = provision_tls(cert_dir=target, host="127.0.0.1")

    assert cert_path == target / "fullchain.pem"
    assert key_path == target / "privkey.pem"
    assert not (target / GENERATED_CERT_NAME).exists()
    cert_bytes, key_bytes = _public_key_bytes(cert_path, key_path)
    assert cert_bytes == key_bytes


def test_provision_rejects_mismatched_certificate_and_key(tmp_path):
    first = tmp_path / "first"
    first.mkdir()
    second = tmp_path / "second"
    second.mkdir()
    provision_tls(cert_dir=first, host="127.0.0.1")
    provision_tls(cert_dir=second, host="127.0.0.1")
    # Replace the canonical certificate with one from another key pair.
    shutil.copy(second / GENERATED_CERT_NAME, first / GENERATED_CERT_NAME)

    with pytest.raises(SecurityConfigurationError, match="do not match"):
        provision_tls(cert_dir=first, host="127.0.0.1")
    # User-provided material is preserved, not overwritten.
    assert (first / GENERATED_CERT_NAME).read_bytes() == (
        second / GENERATED_CERT_NAME
    ).read_bytes()


def test_provision_rejects_unreadable_existing_pair(tmp_path):
    (tmp_path / GENERATED_CERT_NAME).write_text("not a certificate")
    (tmp_path / GENERATED_KEY_NAME).write_text("not a key")

    with pytest.raises(SecurityConfigurationError, match="do not match"):
        provision_tls(cert_dir=tmp_path, host="127.0.0.1")


def test_generated_key_is_private_only(tmp_path):
    provision_tls(cert_dir=tmp_path, host="127.0.0.1")
    mode = stat.S_IMODE((tmp_path / GENERATED_KEY_NAME).stat().st_mode)
    assert mode == 0o600


def test_san_covers_bind_host(tmp_path):
    cert_path, _ = provision_tls(cert_dir=tmp_path, host="research.example")
    sans = _load_cert(cert_path).extensions.get_extension_for_class(
        x509.SubjectAlternativeName
    ).value
    assert x509.DNSName("research.example") in sans


def test_wildcard_bind_host_skipped(tmp_path):
    cert_path, _ = provision_tls(cert_dir=tmp_path, host="0.0.0.0")
    sans = _load_cert(cert_path).extensions.get_extension_for_class(
        x509.SubjectAlternativeName
    ).value
    assert x509.DNSName("0.0.0.0") not in sans
    assert not any(
        isinstance(name, x509.IPAddress)
        and name.value.is_unspecified  # type: ignore[attr-defined]
        for name in sans
    )
