from typing import Dict, Optional
import json
import httpx
import google.cloud
from google.cloud import firestore
import dask.bag as dask_bag

from .config import username, personal_access_token
from .logger import logging


log = logging.getLogger(__name__)


db = firestore.Client.from_service_account_json("./credentials.json")


def search_users_by_location(
    location: str,
    per_page: int = 100,
    page: int = 1,
):
    """Search GitHub API by location."""
    headers = {
        "accept": "application/vnd.github+json",
        "Authorization": "token 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"  # Don't worry, this token is revoked
    }

    with httpx.Client(timeout=60, headers=headers) as client:
        response = client.get(
            f"https://api.github.com/search/users?q=location:{location}&per_page={per_page}&page={page}"
        )

    result = response.json()


    return result


def add_document(
    document_id: str,
    data: Dict,
    collection_id: str,
    db: Optional[google.cloud.firestore_v1.client.Client] = db,
) -> str:
    """
    Add document
    """
    document_id_string = str(document_id)
    document_reference = db.collection(collection_id).document(document_id_string)

    document_reference.set(data, merge=True)

    return document_id_string


def set_document(
    document_id: str,
    document: Dict,
    collection_id: str,
    db: google.cloud.firestore_v1.client.Client = db,
):
    document_id_string = str(document_id)
    document_reference = db.collection(collection_id).document(document_id_string)

    document_reference.set(document, merge=True)

    return document_id_string


def get_document(
    document_id: str,
    collection_id: str,
    db: google.cloud.firestore_v1.client.Client = db,
):
    document_reference = db.collection(collection_id).document(document_id)
    document_snapshot = document_reference.get()
    document = document_snapshot.to_dict()

    return document


def save_to_firestore(item: Dict, collection_id: str = "users"):
    """Save item to firestore."""
    document_id = item["id"]
    result = add_document(document_id=document_id, data=item, collection_id=collection_id)

    return result


def get_events(user_id, page: int = 1, per_page: int = 100, collection_id: str = "user_events"):
    """Get events from GitHub API and save to firestore."""
    try:
        headers = {
            "accept": "application/vnd.github+json",
            "Authorization": "token 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"
        }

        with httpx.Client(timeout=60, headers=headers) as client:
            response = client.get(
                f"https://api.github.com/users/{user_id}/events/public?page={page}&per_page={per_page}"
            )

        events = response.json()

        for event in events:
            # Save repo to firestore
            event_id = event["id"]
            repo = event["repo"]
            repo_id = repo["id"]
            repo_name = repo["name"]
            add_document(repo_id, repo, "repos")

            # Save user repo to firestore
            user_repos = get_document(user_id, "user_repos")
            if not user_repos:
                user_repos = {
                    "id": user_id,
                    "repo_names": [repo_name],
                    "repo_ids": [repo_id]
                }
            else:
                user_repos["repo_names"].append(repo_name)
                user_repos["repo_ids"].append(repo_id)

            # Unique values only
            user_repos["repo_names"] = list(set(user_repos["repo_names"]))
            user_repos["repo_ids"] = list(set(user_repos["repo_ids"]))
            
            set_document(user_id, user_repos, "user_repos")

        return events
    except Exception as exception:
        log.exception(f"Error handling {user_id}")
        return []


def ingest_events(user_id: str = "jthtezel", page: int = 1, per_page: int = 100) -> int:
    """Ingest events from GitHub API by user."""
    total = 0

    while True:
        result = get_events(
            user_id=user_id,
            per_page=per_page,
            page=page
        )
        log.info(f"page: {page}")

        try:
            number_of_items = len(result)
            log.info(number_of_items)
            total = total + number_of_items
        except Exception as exception:
            log.exception(f"Unhandled exception after {total} items at page {page}")

        if len(result) < per_page:
            break

        page = page + 1

    # Summarize user
    summarize_user(user_id=user_id, total_commits=total)

    log.info(f"Done after {page} pages")


def summarize_user(user_id: str, total_commits: int):
    """Create summary statistics for user."""
    user_repos = get_document(user_id, "user_repos")
    profile = get_document(user_id, "profiles")
    if user_repos:
        total_repos = len(user_repos["repo_ids"])
        user_summary = {
            "id": user_id,  # GitHub "login"
            "uuid": profile["id"],  # GitHub "id"
            "name": profile["name"],
            "created_at": profile["created_at"],
            "html_url": profile["html_url"],
            "company": profile["company"],
            "bio": profile["bio"],
            "blog": profile["blog"],
            "avatar_url": profile["avatar_url"],
            "total_commits": total_commits,
            "total_repos": total_repos,
        }

        result = add_document(user_id, user_summary, "user_summaries")

        return result
        


def batch_ingest_events(limit: int = 2, input_collection: str = "users", output_collection: str = "profiles"):
    """Ingest GitHub user events from collection."""
    snapshots = db.collection(input_collection).limit(limit).get()
    documents = [snapshot.to_dict() for snapshot in snapshots]
    log.info(f"ingesting {len(documents)} documents")

    documents_bag = dask_bag.from_sequence(documents)
    results = documents_bag.map(lambda document: ingest_events(document["login"])).compute()
    
    return results


def summary_json(limit: int = 2000, input_collection: str = "user_summaries"):
    """Ingest GitHub user events from collection."""
    snapshots = db.collection(input_collection).limit(limit).get()
    documents = [snapshot.to_dict() for snapshot in snapshots]
    log.info(f"ingesting {len(documents)} documents")

    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(documents, f, ensure_ascii=False, indent=4)
    
    return "data.json"


def profile_json(limit: int = 2000, input_collection: str = "profiles"):
    """Ingest GitHub user events from collection."""
    snapshots = db.collection(input_collection).limit(limit).get()
    documents = [snapshot.to_dict() for snapshot in snapshots]
    log.info(f"ingesting {len(documents)} documents")

    with open('profile.json', 'w', encoding='utf-8') as f:
        json.dump(documents, f, ensure_ascii=False, indent=4)
    
    return "profile.json"

    
def get_profile(document, collection_id: str = "profiles"):
    """Get profile from GitHub API and save to firestore."""
    try:
        document_id = document["login"]

        headers = {
            "accept": "application/vnd.github+json",
            "Authorization": "token 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"
        }

        with httpx.Client(timeout=60, headers=headers) as client:
            response = client.get(
                f"https://api.github.com/users/{document_id}"
            )

        result = response.json()

        # Save profile to firestore
        add_document(document_id, result, collection_id)

        # Save location to firestore
        location_id = result["location"]
        log.info(location_id)
        location_document = get_document(location_id, "locations")
        if not location_document:
            location_document = {
                "id": location_id,
                "count": 1
            }
        else:
            location_document["count"] = location_document["count"] + 1

        set_document(location_id, location_document, "locations")

        return result
    except Exception as exception:
        log.exception(f"Error handling {document_id}")

        return {}
    

def ingest_profiles(limit: int = 10, input_collection: str = "users", output_collection: str = "profiles"):
    """Ingest GitHub user profiles from collection."""
    snapshots = db.collection(input_collection).limit(limit).get()
    documents = [snapshot.to_dict() for snapshot in snapshots]
    log.info(f"ingesting {len(documents)} documents")

    documents_bag = dask_bag.from_sequence(documents)
    results = documents_bag.map(lambda document: get_profile(document, output_collection)).compute()
    return results


def ingest(location: str = "newfoundland", page: int = 1, per_page: int = 100) -> int:
    """Ingest users from GitHub API by location."""
    total = 0

    while True:
        result = search_users_by_location(
            # location="canada",
            location="newfoundland",
            per_page=per_page,
            page=page
        )
        log.info(f"page: {page}")
        if "items" not in result.keys():
            breakpoint()
        try:
            number_of_items = len(result["items"])
            log.info(number_of_items)
            total = total + number_of_items
        except Exception as exception:
            breakpoint()
            log.exception(f"Unhandled exception after {total} items at page {page}")
        page = page + 1
        for item in result["items"]:
            save_to_firestore(item)

        if len(result["items"]) < per_page:
            break

    log.info(f"Done after {page} pages")


if __name__ == "__main__":
    page = 1
    per_page = 100
    total = 0

    while True:
        result = search_users_by_location(
            # location="canada",
            location="newfoundland",
            per_page=per_page,
            page=page
        )
        log.info(f"page: {page}")
        if "items" not in result.keys():
            breakpoint
        try:
            log.info(len(result["items"]))
        except Exception as exception:
            breakpoint()
            log.exception("Unhandled exception")
        page = page + 1
        for item in result["items"]:
            save_to_firestore(item)

        if len(result["items"]) < per_page:
            break

    log.info(f"Done after {page} pages")






