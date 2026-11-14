"""Shared-storage helpers. All task code goes through this module so the
backing store could later be swapped (e.g. for HDFS)."""
import os
import uuid
from contextlib import contextmanager

ROOT = os.path.abspath(os.environ.get("SHARED_DIR", "/shared"))

# Containers run as root but ./shared belongs to the host user. A umask of 0
# makes everything we create world-readable/writable, so the host user can
# still read, overwrite and delete job outputs without sudo.
os.umask(0)


def prepare_root():
    """Create the shared directory layout and make it writable by the host
    user (Docker creates a missing bind-mount directory as root-owned)."""
    for sub in ("", "input", "jobs", "output"):
        d = os.path.join(ROOT, sub) if sub else ROOT
        os.makedirs(d, exist_ok=True)
        try:
            os.chmod(d, 0o777)
        except OSError:
            pass


def resolve(path):
    """Map a storage path like '/input/x' to a real filesystem path."""
    full = os.path.normpath(os.path.join(ROOT, str(path).lstrip("/")))
    if full != ROOT and not full.startswith(ROOT + os.sep):
        raise ValueError(f"path escapes shared storage: {path}")
    return full


def exists(path):
    return os.path.exists(resolve(path))


def list_files(path, suffix=None):
    """Return sorted [(storage_path, size)] for a file or directory (recursive)."""
    full = resolve(path)
    out = []
    if os.path.isfile(full):
        out.append(full)
    elif os.path.isdir(full):
        for d, _, names in os.walk(full):
            out += [os.path.join(d, n) for n in names]
    result = []
    for f in sorted(out):
        if ".tmp-" in os.path.basename(f):
            continue
        if suffix and not f.lower().endswith(suffix):
            continue
        result.append((os.path.relpath(f, ROOT), os.path.getsize(f)))
    return result


def open_read(path):
    return open(resolve(path), "rb")


@contextmanager
def atomic_open(path):
    """Write to a temp file, then os.rename() into place on success."""
    full = resolve(path)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    tmp = f"{full}.tmp-{uuid.uuid4().hex}"
    f = open(tmp, "w", encoding="utf-8")
    try:
        yield f
        f.close()
        os.rename(tmp, full)
    except BaseException:
        f.close()
        if os.path.exists(tmp):
            os.remove(tmp)
        raise


def write_atomic(path, text):
    with atomic_open(path) as f:
        f.write(text)
