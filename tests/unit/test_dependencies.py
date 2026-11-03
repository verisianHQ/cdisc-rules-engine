import tomllib
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

ROOT = Path(__file__).parents[2]
# Needed to build the package, not to run it, so pyproject.toml leaves it out
BUILD_ONLY = {"wheel"}


def _read_requirements(filename: str) -> list[Requirement]:
    lines = (line.strip() for line in (ROOT / filename).read_text().splitlines())
    return [Requirement(line) for line in lines if line and not line.startswith(("#", "-"))]


def test_pyproject_dependencies_match_requirements():
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text())
    from_pyproject = {str(Requirement(dep)) for dep in pyproject["project"]["dependencies"]}
    from_requirements = {
        str(req) for req in _read_requirements("requirements.txt") if canonicalize_name(req.name) not in BUILD_ONLY
    }
    assert from_pyproject == from_requirements


def test_constraints_leave_out_direct_requirements():
    direct = {
        canonicalize_name(req.name)
        for filename in ("requirements.txt", "requirements-dev.txt")
        for req in _read_requirements(filename)
    }
    constrained = {canonicalize_name(req.name) for req in _read_requirements("constraints.txt")}
    assert not direct & constrained
