"""The mock repository the code agents fix, generated from a seed.

It is a small Python package, `mathlib`, whose modules hold functions of one
number, such as

    def f_03_b(x):
        return x * 4 + 7

and every function has a test in tests/ that checks one value, with a
message that says what the value should be and what it was. A few constants
are planted wrong, so those tests fail, and the run has to make the whole
check pass on a branch of its own without touching the tests.

One planted bug sits in a helper, `scale` in helpers.py, which a function in
each of two modules uses, so three test files fail for that one cause, and
fixing the helper fixes all three, which a good fix finds by checking the
helper's own test before it changes a function. One module is long enough to
be read in several pages, and with --goal all one of the bugs sits in its
middle and another sits in a module that also uses the broken helper, so
that module has two causes.
"""

import random
from dataclasses import dataclass, field

PACKAGE = "mathlib"
DEFAULT_MODULES = 12
LONG_FUNCTIONS = 60  # functions in the long module, enough for several pages of reading
HELPER_USERS = 2  # modules with a function that uses scale
PLAIN_BUGS = {"one": 1, "all": 3}  # wrong constants besides the helper's, per goal
# The check the mock repository is fixed against, where {python} is the
# interpreter that runs the system, which has pytest, and {item} is one test file.
CHECK = "{python} -m pytest -q --tb=no -rf --no-header -p no:cacheprovider {item}"
PROTECTED = ("tests/*",)  # what a fix may not edit, so it can't pass by changing a test


@dataclass
class Function:
    name: str
    module: str  # such as 'mod_03'
    a: int
    b: int
    uses_scale: bool = False
    x: int = 2  # the value its test checks

    def body(self, a: int | None = None, b: int | None = None) -> str:
        a, b = self.a if a is None else a, self.b if b is None else b
        expr = f"scale(x) + {b}" if self.uses_scale else f"x * {a} + {b}"
        return f"def {self.name}(x):\n    return {expr}\n"


@dataclass
class Bug:
    path: str  # the file the wrong constant is in
    function: str
    right: str  # the source of the function as it should be
    wrong: str  # and as it is


@dataclass
class MockRepo:
    seed: int
    size: int
    files: dict[str, str] = field(default_factory=dict)  # path -> text, as committed at the start
    bugs: list[Bug] = field(default_factory=list)
    failing: list[str] = field(default_factory=list)  # the test files that fail at the start
    long_module: str = ""


def generate_repo(seed: int, size: int = DEFAULT_MODULES, goal: str = "one") -> MockRepo:
    if size < HELPER_USERS + PLAIN_BUGS["all"] + 2:
        raise ValueError(f"need at least {HELPER_USERS + PLAIN_BUGS['all'] + 2} modules")
    rng = random.Random(f"repo|{seed}|{size}")
    repo = MockRepo(seed=seed, size=size)
    k = rng.randint(2, 9)  # what scale(x) multiplies by
    modules = [f"mod_{i:02d}" for i in range(1, size + 1)]
    repo.long_module = rng.choice(modules)
    others = [m for m in modules if m != repo.long_module]
    users = rng.sample(others, HELPER_USERS)
    functions: dict[str, list[Function]] = {}
    for module in modules:
        n = LONG_FUNCTIONS if module == repo.long_module else rng.randint(3, 5)
        functions[module] = [
            Function(
                f"f_{module[4:]}_{_letters(i)}",
                module,
                rng.randint(2, 9),
                rng.randint(0, 20),
                x=rng.randint(2, 9),
            )
            for i in range(n)
        ]
        if module in users:
            rng.choice(functions[module]).uses_scale = True

    # --- the planted bugs ---------------------------------------------------
    bugs = random.Random(f"bugs|{seed}|{size}|{goal}")
    wrong_k = bugs.choice([v for v in range(2, 10) if v != k])
    planted: dict[str, int] = {}  # function name -> its wrong b
    long_fns = functions[repo.long_module]
    if goal == "all":
        middle = long_fns[len(long_fns) // 3 : 2 * len(long_fns) // 3]
        planted[bugs.choice(middle).name] = 0
        user_fns = [f for f in functions[bugs.choice(users)] if not f.uses_scale]
        planted[bugs.choice(user_fns).name] = 0
    plain = [f for m in others if m not in users for f in functions[m]]
    while len(planted) < PLAIN_BUGS[goal]:
        planted[bugs.choice(plain).name] = 0
    by_name = {f.name: f for fns in functions.values() for f in fns}
    for name in planted:
        f = by_name[name]
        planted[name] = f.b + bugs.choice([-5, -4, -3, -2, -1, 1, 2, 3, 4, 5])

    # --- the files ------------------------------------------------------------
    repo.files["pyproject.toml"] = (
        f'[project]\nname = "{PACKAGE}"\nversion = "0.0.1"\n\n[tool.pytest.ini_options]\npythonpath = ["."]\n'
    )
    repo.files["README.md"] = f"# {PACKAGE}\n\nFunctions of one number, each with a test in tests/.\n"
    repo.files[f"{PACKAGE}/__init__.py"] = '"""Functions of one number."""\n'
    repo.files[f"{PACKAGE}/helpers.py"] = f"def scale(x):\n    return x * {wrong_k}\n"
    repo.bugs.append(
        Bug(
            f"{PACKAGE}/helpers.py",
            "scale",
            f"def scale(x):\n    return x * {k}\n",
            f"def scale(x):\n    return x * {wrong_k}\n",
        )
    )
    repo.files["tests/test_helpers.py"] = f"from {PACKAGE}.helpers import scale\n\n\n" + _test(
        "scale", 3, 3 * k
    )
    for module, fns in functions.items():
        path = f"{PACKAGE}/{module}.py"
        head = f"from {PACKAGE}.helpers import scale\n\n\n" if any(f.uses_scale for f in fns) else ""
        bodies = []
        for f in fns:
            if f.name in planted:
                repo.bugs.append(Bug(path, f.name, f.body(), f.body(b=planted[f.name])))
                bodies.append(f.body(b=planted[f.name]))
            else:
                bodies.append(f.body())
        repo.files[path] = head + "\n\n".join(bodies)
        names = ", ".join(f.name for f in fns)
        tests = "\n\n".join(_test(f.name, f.x, (f.x * k if f.uses_scale else f.x * f.a) + f.b) for f in fns)
        repo.files[f"tests/test_{module}.py"] = f"from {PACKAGE}.{module} import {names}\n\n\n{tests}"
    failing = {"tests/test_helpers.py"} | {f"tests/test_{m}.py" for m in users}
    failing |= {f"tests/test_{by_name[name].module}.py" for name in planted}
    repo.failing = sorted(failing)
    return repo


def _test(name: str, x: int, expected: int) -> str:
    return (
        f"def test_{name}():\n"
        f"    got = {name}({x})\n"
        f'    assert got == {expected}, f"{name}({x}) should be {expected}, got {{got}}"\n'
    )


def _letters(i: int) -> str:
    """a, b, ..., z, ba, bb, ... for the functions of a module."""
    out = ""
    while True:
        out = "abcdefghijklmnopqrstuvwxyz"[i % 26] + out
        i //= 26
        if i == 0:
            return out
