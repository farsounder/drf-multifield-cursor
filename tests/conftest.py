import django


def pytest_configure(config):
    from django.conf import settings

    settings.configure(
        DEBUG_PROPAGATE_EXCEPTIONS=True,
        DATABASES={
            "default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"},
        },
        SECRET_KEY="not very secret in tests",
        INSTALLED_APPS=(
            "django.contrib.auth",
            "django.contrib.contenttypes",
            "rest_framework",
            "drf_multifield_cursor",
            "tests",
        ),
        ALLOWED_HOSTS=["*"],
    )
    django.setup()
