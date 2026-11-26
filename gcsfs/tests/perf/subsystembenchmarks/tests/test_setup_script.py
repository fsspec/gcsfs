import os
import subprocess
import sys

_REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..")
)
_INSTALL_GROUP = os.path.join(
    _REPO_ROOT, "cloudbuild", "subsystembenchmarks", "scripts", "install-group.sh"
)


def _make_fake_tpu_sysfs(tmp_path):
    pci_root = tmp_path / "pci"
    dev = pci_root / "0000:00:04.0"
    dev.mkdir(parents=True)
    (dev / "vendor").write_text("0x1ae0\n")
    (dev / "device").write_text("0x0076\n")
    # Point iommu_group to a readable/writable char device (/dev/null).
    iommu_dir = tmp_path / "iommu_groups" / "null"
    iommu_dir.mkdir(parents=True)
    os.symlink(str(iommu_dir), dev / "iommu_group")
    return str(pci_root), "/dev"


def test_setup_installs_torch_tpu_only_on_tpu_hosts(tmp_path):
    assert os.path.isfile(_INSTALL_GROUP), f"missing {_INSTALL_GROUP}"
    subprocess.run(["bash", "-n", _INSTALL_GROUP], check=True)

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    pip_log = tmp_path / "pip_calls.log"
    fake_pip = bin_dir / "pip"
    fake_pip.write_text(f'#!/usr/bin/env bash\necho "$@" >> "{pip_log}"\n')
    fake_pip.chmod(0o755)

    # Stub python3 that reports torch_tpu is NOT installed.
    fake_py = bin_dir / "python3"
    fake_py.write_text(f'#!/usr/bin/env bash\nexec "{sys.executable}" "$@"\n')
    fake_py.chmod(0o755)

    # 1. Non-TPU host: no torch_tpu install.
    empty_pci = tmp_path / "empty_pci"
    empty_pci.mkdir()
    env_cpu = {
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "PCI_DEVICES_ROOT": str(empty_pci),
        "VFIO_DEV_ROOT": "/dev/vfio",
    }
    subprocess.run(
        [
            "bash",
            "-c",
            f'source "{_INSTALL_GROUP}" && install_accelerator_packages',
        ],
        env=env_cpu,
        check=True,
    )
    assert not pip_log.exists() or "torch_tpu" not in pip_log.read_text()

    # 2. TPU host (fake sysfs + char device): installs torch_tpu.
    pci_root, vfio_root = _make_fake_tpu_sysfs(tmp_path)
    env_tpu = {
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "PCI_DEVICES_ROOT": pci_root,
        "VFIO_DEV_ROOT": vfio_root,
    }
    subprocess.run(
        [
            "bash",
            "-c",
            f'source "{_INSTALL_GROUP}" && install_accelerator_packages',
        ],
        env=env_tpu,
        check=True,
    )
    calls = pip_log.read_text().splitlines()
    assert any("torch_tpu" in c for c in calls), calls


def test_install_group_preserves_debian_no_record_packages(tmp_path):
    """When running outside a venv with preinstalled torch, packages installed without
    a RECORD file (e.g. Debian PyYAML in NGC containers) must not be upgraded by pip."""
    fake_site = tmp_path / "site"
    fake_site.mkdir()
    # Fake torch dist WITH RECORD
    torch_info = fake_site / "torch-2.10.0.dist-info"
    torch_info.mkdir()
    (torch_info / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: torch\nVersion: 2.10.0\n"
    )
    (torch_info / "RECORD").write_text("torch/__init__.py,,\n")
    # Fake Debian PyYAML dist WITHOUT RECORD
    yaml_info = fake_site / "PyYAML-6.0.1.egg-info"
    yaml_info.mkdir()
    (yaml_info / "PKG-INFO").write_text(
        "Metadata-Version: 1.1\nName: PyYAML\nVersion: 6.0.1\n"
    )

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    captured_req = tmp_path / "captured_req.txt"
    captured_con = tmp_path / "captured_con.txt"
    fake_pip = bin_dir / "pip"
    fake_pip.write_text(f"""#!/usr/bin/env bash
set -euo pipefail
cp "$PIP_CONSTRAINT" "{captured_con}"
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "-r" ]]; then
    cp "$2" "{captured_req}"
    shift 2
  else
    shift
  fi
done
""")
    fake_pip.chmod(0o755)
    # Fake python wrapper that exposes fake_site and simulates sys.prefix == sys.base_prefix.
    fake_py = bin_dir / "python"
    fake_py.write_text(
        f"#!{sys.executable}\n"
        "import sys\n"
        f"sys.path.insert(0, {str(fake_site)!r})\n"
        "sys.prefix = sys.base_prefix\n"
        "args = sys.argv[1:]\n"
        "if args[:1] == ['-c']:\n"
        "    exec(args[1])\n"
        "else:\n"
        "    sys.argv[:] = args\n"
        "    exec(sys.stdin.read())\n"
    )
    fake_py.chmod(0o755)

    env = {
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "GROUP": "dataloading/webdataset",
    }
    subprocess.run(
        [
            "bash",
            "-c",
            f'cd "{_REPO_ROOT}" && source "{_INSTALL_GROUP}" && install_group_requirements',
        ],
        env=env,
        check=True,
    )
    req_lines = captured_req.read_text().lower().splitlines()
    con_lines = captured_con.read_text().lower().splitlines()
    assert not any(line.startswith("pyyaml") for line in req_lines), req_lines
    assert any("pyyaml==6.0.1" in line for line in con_lines), con_lines
