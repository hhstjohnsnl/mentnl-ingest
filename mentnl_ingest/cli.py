"""Command line interface for mentnl ingest."""
import typer

from .github import ingest as ingest_github, ingest_profiles, ingest_events, batch_ingest_events, summary_json, profile_json


app = typer.Typer()


@app.command()
def ingest(location: str = "newfoundland", page: int = 1, per_page: int = 100) -> int:
    """Ingest users from GitHub API by location."""
    result = ingest_github(location, page, per_page)

    return result


@app.command()
def profiles(limit: int = 2000, input_collection: str = "users", output_collection: str = "profiles"):
    result = ingest_profiles(limit, input_collection, output_collection)

    return result


@app.command()
def events(user_id: str = "jthetzel", page: int = 1, per_page: int = 100) -> int:
    """Ingest events from GitHub API by user."""
    result = ingest_events(user_id, page, per_page)

    return result

@app.command()
def batch_events(limit: int = 2000) -> int:
    """Batch ingest events from GitHub API by all users."""
    result = batch_ingest_events(limit=limit)

    return result


@app.command()
def summary(limit: int = 2000) -> int:
    """Batch ingest events from GitHub API by all users."""
    result = summary_json(limit=limit)

    return result


@app.command()
def profile(limit: int = 2000) -> int:
    """Batch ingest events from GitHub API by all users."""
    result = profile_json(limit=limit)

    return result


if __name__ == "__main__":
    app()
