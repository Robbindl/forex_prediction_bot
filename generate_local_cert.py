from __future__ import annotations

import argparse
import ipaddress
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def _is_ip(value: str) -> bool:
    try:
        ipaddress.ip_address(value)
        return True
    except ValueError:
        return False


def _openssl_available() -> bool:
    return shutil.which("openssl") is not None or shutil.which("openssl.exe") is not None


def _build_openssl_config(common_name: str, san: list[str]) -> str:
    config_lines = [
        "[req]",
        "distinguished_name = req_distinguished_name",
        "x509_extensions = v3_req",
        "prompt = no",
        "",
        "[req_distinguished_name]",
        f"CN = {common_name}",
        "",
        "[v3_req]",
        "keyUsage = keyEncipherment, digitalSignature",
        "extendedKeyUsage = serverAuth",
        "subjectAltName = @alt_names",
        "",
        "[alt_names]",
    ]

    for index, value in enumerate(sorted(set(san)), start=1):
        kind = "IP" if _is_ip(value) else "DNS"
        config_lines.append(f"{kind}.{index} = {value}")

    config_path = tempfile.mkstemp(prefix="openssl-sans-", suffix=".cnf")[1]
    with open(config_path, "w", encoding="utf-8") as fp:
        fp.write("\n".join(config_lines) + "\n")
    return config_path


def _generate_with_openssl(cert_path: Path, key_path: Path, common_name: str, san: list[str], days: int) -> bool:
    config_path = _build_openssl_config(common_name, san)
    try:
        cmd = [
            shutil.which("openssl") or shutil.which("openssl.exe"),
            "req",
            "-x509",
            "-nodes",
            "-newkey",
            "rsa:2048",
            "-keyout",
            str(key_path),
            "-out",
            str(cert_path),
            "-days",
            str(days),
            "-config",
            config_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print("OpenSSL failed:", result.stderr.strip(), file=sys.stderr)
            return False
        return True
    finally:
        try:
            os.remove(config_path)
        except OSError:
            pass


def _generate_with_cryptography(cert_path: Path, key_path: Path, common_name: str, san: list[str], days: int) -> bool:
    try:
        from cryptography import x509
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID
    except ImportError:
        return False

    import datetime

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048, backend=default_backend())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    builder = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.utcnow())
        .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=days))
        .add_extension(
            x509.SubjectAlternativeName([
                x509.DNSName(value) if not _is_ip(value) else x509.IPAddress(ipaddress.ip_address(value))
                for value in sorted(set(san))
            ]),
            critical=False,
        )
    )
    cert = builder.sign(private_key=private_key, algorithm=hashes.SHA256(), backend=default_backend())

    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    return True


def generate_certificate(cert_path: Path, key_path: Path, common_name: str, san: list[str], days: int) -> None:
    if cert_path.exists() or key_path.exists():
        raise FileExistsError("Destination files already exist. Remove them first or choose another path.")

    if _openssl_available():
        success = _generate_with_openssl(cert_path, key_path, common_name, san, days)
        if success:
            print(f"Created cert={cert_path} key={key_path} using OpenSSL")
            return
        print("OpenSSL was found but failed to generate certificate.")

    if _generate_with_cryptography(cert_path, key_path, common_name, san, days):
        print(f"Created cert={cert_path} key={key_path} using cryptography")
        return

    raise RuntimeError(
        "No certificate generator is available. Install OpenSSL or the Python package 'cryptography'."
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a local self-signed certificate for HTTP/2 HTTPS testing."
    )
    parser.add_argument("--cert", type=Path, default=Path("cert.pem"), help="Output certificate path")
    parser.add_argument("--key", type=Path, default=Path("key.pem"), help="Output private key path")
    parser.add_argument("--cn", type=str, default="localhost", help="Common Name for the certificate")
    parser.add_argument(
        "--san",
        type=str,
        default="localhost,127.0.0.1",
        help="Comma-separated Subject Alternative Names",
    )
    parser.add_argument("--days", type=int, default=365, help="Certificate validity in days")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    san = [item.strip() for item in args.san.split(",") if item.strip()]
    try:
        generate_certificate(args.cert, args.key, args.cn, san, args.days)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
