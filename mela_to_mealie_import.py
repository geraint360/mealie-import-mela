#!/usr/bin/env python3
"""Import recipes from Mela exports into Mealie with a review-first workflow.

Defaults are intentionally conservative:
- `--dry-run` does not call Mealie at all
- `--limit` lets you test a handful of recipes first
- only the selected recipes are scanned for categories/tags
- images are uploaded only when live mode is used and `--skip-images` is not set
"""

from __future__ import annotations

import argparse
import base64
import json
import re
import sys
import tempfile
import time
import uuid
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

try:
    import requests
except ImportError:
    print("Missing dependency: pip install requests")
    sys.exit(1)


def slugify(text: str) -> str:
    import unicodedata

    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text.strip("-")


def parse_time_to_iso(time_str: str | None) -> str | None:
    if not time_str:
        return None

    value = time_str.strip()
    if not value:
        return None

    lowered = value.lower()
    if lowered.startswith("pt"):
        return value.upper()

    hours = 0
    minutes = 0

    hours_match = re.search(r"(\d+)\s*(?:h(?:ours?|r)?|stunde[n]?)", lowered)
    minutes_match = re.search(r"(\d+)\s*(?:m(?:in(?:ute[ns]?)?)?|minute[n]?)", lowered)

    if hours_match:
        hours = int(hours_match.group(1))
    if minutes_match:
        minutes = int(minutes_match.group(1))

    if not hours_match and not minutes_match:
        bare = re.fullmatch(r"(\d+)", lowered)
        if bare:
            minutes = int(bare.group(1))
        else:
            return value

    if hours == 0 and minutes == 0:
        return value

    parts = "PT"
    if hours:
        parts += f"{hours}H"
    if minutes:
        parts += f"{minutes}M"
    return parts


def instructions_to_steps(text: str | None) -> list[dict]:
    if not text:
        return []

    steps: list[dict] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        step = {
            "id": str(uuid.uuid4()),
            "title": "",
            "summary": "",
            "text": "",
            "ingredientReferences": [],
        }

        if line.startswith("#"):
            step["title"] = line.lstrip("#").strip()
        else:
            line = re.sub(r"^\d+\.\s*", "", line)
            step["text"] = line

        steps.append(step)

    return steps


def ingredients_to_list(text: str | None) -> list[dict]:
    if not text:
        return []

    items: list[dict] = []
    pending_title: str | None = None

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        if line.startswith("#"):
            pending_title = line.lstrip("#").strip()
            continue

        item = {
            "note": line,
            "referenceId": str(uuid.uuid4()),
        }
        if pending_title:
            item["title"] = pending_title
            pending_title = None
        items.append(item)

    return items


def detect_image_extension(data: bytes) -> str:
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "png"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "webp"
    if data[:3] == b"\xff\xd8\xff":
        return "jpg"
    return "jpg"


def decode_first_image(recipe: dict) -> tuple[bytes, str] | None:
    images = recipe.get("images") or []
    if not images:
        return None

    try:
        data = base64.b64decode(images[0])
    except Exception:
        return None

    return data, detect_image_extension(data)


def recipe_date_fields(mela_recipe: dict) -> dict:
    raw = mela_recipe.get("date")
    if raw is None:
        return {}

    base = datetime(2001, 1, 1, tzinfo=timezone.utc)
    dt = base + timedelta(seconds=raw)
    return {
        "dateAdded": dt.strftime("%Y-%m-%d"),
        "createdAt": dt.isoformat(),
    }


def notes_from_recipe(mela_recipe: dict) -> list[dict]:
    notes: list[dict] = []

    if mela_recipe.get("notes"):
        notes.append({"title": "Notes", "text": mela_recipe["notes"]})
    if mela_recipe.get("nutrition"):
        notes.append({"title": "Nutrition", "text": mela_recipe["nutrition"]})

    return notes


def selected_tags(mela_recipe: dict, tag_lookup: dict) -> list[dict]:
    tags: list[dict] = []
    for slug in ("mela-import",):
        if slug in tag_lookup:
            tags.append(tag_lookup[slug])
    if mela_recipe.get("favorite") and "favorite" in tag_lookup:
        tags.append(tag_lookup["favorite"])
    if mela_recipe.get("wantToCook") and "want-to-cook" in tag_lookup:
        tags.append(tag_lookup["want-to-cook"])
    return tags


def selected_categories(mela_recipe: dict, category_lookup: dict) -> list[dict]:
    refs: list[dict] = []
    for category in mela_recipe.get("categories") or []:
        if not category:
            continue
        slug = slugify(category)
        if slug in category_lookup:
            refs.append(category_lookup[slug])
    return refs


def build_recipe_payload(mela_recipe: dict, category_lookup: dict, tag_lookup: dict) -> dict:
    payload: dict = {
        "name": mela_recipe.get("title") or "Untitled",
        "recipeIngredient": ingredients_to_list(mela_recipe.get("ingredients")),
        "recipeInstructions": instructions_to_steps(mela_recipe.get("instructions")),
    }

    if mela_recipe.get("text"):
        payload["description"] = mela_recipe["text"]
    if mela_recipe.get("yield"):
        payload["recipeYield"] = mela_recipe["yield"]
    if mela_recipe.get("link"):
        payload["orgURL"] = mela_recipe["link"]

    prep = parse_time_to_iso(mela_recipe.get("prepTime"))
    if prep:
        payload["prepTime"] = prep
    cook = parse_time_to_iso(mela_recipe.get("cookTime"))
    if cook:
        payload["performTime"] = cook
    total = parse_time_to_iso(mela_recipe.get("totalTime"))
    if total:
        payload["totalTime"] = total

    date_fields = recipe_date_fields(mela_recipe)
    payload.update(date_fields)

    notes = notes_from_recipe(mela_recipe)
    if notes:
        payload["notes"] = notes

    categories = selected_categories(mela_recipe, category_lookup)
    if categories:
        payload["recipeCategory"] = categories

    tags = selected_tags(mela_recipe, tag_lookup)
    if tags:
        payload["tags"] = tags

    return payload


@dataclass
class RecipeEntry:
    index: int
    archive_name: str
    recipe: dict


def iter_mela_archive(export_path: Path) -> Iterable[RecipeEntry]:
    if export_path.suffix == ".melarecipe":
        with export_path.open("r", encoding="utf-8") as handle:
            yield RecipeEntry(0, export_path.name, json.load(handle))
        return

    if export_path.suffix != ".melarecipes":
        raise ValueError(f"Unsupported file type: {export_path.suffix}")

    next_index = 0
    with zipfile.ZipFile(export_path, "r") as zf:
        for name in zf.namelist():
            if name.endswith(".melarecipe"):
                with zf.open(name) as handle:
                    yield RecipeEntry(next_index, name, json.loads(handle.read()))
                    next_index += 1
            elif name.endswith(".melarecipes"):
                with zf.open(name) as nested_handle:
                    nested_data = nested_handle.read()
                with tempfile.NamedTemporaryFile(suffix=".melarecipes", delete=False) as tmp:
                    tmp.write(nested_data)
                    tmp_path = Path(tmp.name)
                try:
                    for nested_entry in iter_mela_archive(tmp_path):
                        yield RecipeEntry(next_index, nested_entry.archive_name, nested_entry.recipe)
                        next_index += 1
                finally:
                    tmp_path.unlink(missing_ok=True)


def load_selected_entries(export_path: Path, limit: int | None, offset: int) -> list[RecipeEntry]:
    selected: list[RecipeEntry] = []

    for index, entry in enumerate(iter_mela_archive(export_path)):
        if index < offset:
            continue
        selected.append(entry)
        if limit is not None and len(selected) >= limit:
            break

    return selected


def count_archive_entries(export_path: Path) -> int:
    count = 0
    for _ in iter_mela_archive(export_path):
        count += 1
    return count


def recipe_identity(entry: RecipeEntry) -> str:
    recipe_id = entry.recipe.get("id")
    if recipe_id:
        return str(recipe_id)
    return entry.archive_name


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_json(path: Path, default):
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, data) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, sort_keys=True)
        handle.write("\n")


def append_log(log_path: Path, payload: dict) -> None:
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False))
        handle.write("\n")


def iter_log_records(log_path: Path) -> Iterable[dict]:
    if not log_path.exists():
        return
    with log_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def default_state_path(export_path: Path) -> Path:
    return export_path.with_suffix(export_path.suffix + ".import-state.json")


def default_log_path(export_path: Path) -> Path:
    return export_path.with_suffix(export_path.suffix + ".import-log.jsonl")


def make_default_state(export_path: Path) -> dict:
    return {
        "export_file": str(export_path),
        "created_at": now_utc_iso(),
        "updated_at": now_utc_iso(),
        "last_scanned_index": -1,
        "processed": {},
        "slug_map": {},
    }


def load_state(state_path: Path, export_path: Path) -> dict:
    state = load_json(state_path, None)
    if state is None:
        state = make_default_state(export_path)
        write_json(state_path, state)
        return state

    state.setdefault("export_file", str(export_path))
    state.setdefault("created_at", now_utc_iso())
    state.setdefault("updated_at", now_utc_iso())
    state.setdefault("last_scanned_index", -1)
    state.setdefault("processed", {})
    state.setdefault("slug_map", {})
    return state


def save_state(state_path: Path, state: dict) -> None:
    state["updated_at"] = now_utc_iso()
    write_json(state_path, state)


def processed_successfully(state: dict, identity: str) -> bool:
    item = state.get("processed", {}).get(identity)
    return bool(item and item.get("status") in {"imported", "skipped_existing"})


def failed_identities(state: dict) -> set[str]:
    return {
        identity
        for identity, item in state.get("processed", {}).items()
        if item.get("status") == "failed"
    }


def record_state(
    state: dict,
    entry: RecipeEntry,
    status: str,
    slug: str | None = None,
    error: str | None = None,
) -> None:
    identity = recipe_identity(entry)
    state["last_scanned_index"] = max(state.get("last_scanned_index", -1), entry.index)
    state["processed"][identity] = {
        "index": entry.index,
        "archive_name": entry.archive_name,
        "title": entry.recipe.get("title") or "Untitled",
        "status": status,
        "slug": slug,
        "error": error,
        "updated_at": now_utc_iso(),
    }
    if status in {"imported", "skipped_existing"} and slug:
        state["slug_map"][identity] = slug


def recorded_slug(state: dict, identity: str) -> str | None:
    slug = state.get("slug_map", {}).get(identity)
    if slug:
        return slug
    item = state.get("processed", {}).get(identity)
    if item:
        return item.get("slug")
    return None


def stream_entries(
    export_path: Path,
    state: dict,
    batch_size: int,
    start_index: int,
    retry_failed_only: bool,
) -> list[RecipeEntry]:
    selected: list[RecipeEntry] = []
    failed_only = failed_identities(state) if retry_failed_only else set()
    for entry in iter_mela_archive(export_path):
        if entry.index < start_index:
            continue
        identity = recipe_identity(entry)
        if retry_failed_only:
            if identity not in failed_only:
                continue
        elif processed_successfully(state, identity):
            state["last_scanned_index"] = max(state.get("last_scanned_index", -1), entry.index)
            continue
        selected.append(entry)
        if len(selected) >= batch_size:
            break
    return selected


class MealieClient:
    def __init__(self, base_url: str, token: str):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            }
        )

    def verify(self) -> dict:
        response = self.session.get(f"{self.base_url}/api/app/about", timeout=30)
        response.raise_for_status()
        return response.json()

    def get_or_create_organizer(self, kind: str, name: str) -> dict:
        slug = slugify(name)
        existing = self.session.get(
            f"{self.base_url}/api/organizers/{kind}/slug/{slug}",
            timeout=30,
        )
        if existing.status_code == 200:
            data = existing.json()
            return {"id": data["id"], "name": data["name"], "slug": data["slug"]}

        if existing.status_code not in (404,):
            existing.raise_for_status()

        create = self.session.post(
            f"{self.base_url}/api/organizers/{kind}",
            json={"name": name},
            timeout=30,
        )
        if create.status_code in (200, 201):
            data = create.json()
            return {"id": data["id"], "name": data["name"], "slug": data["slug"]}

        if create.status_code in (409, 500):
            # Some Mealie versions return 500 instead of 409 for duplicate organizers.
            retry_existing = self.session.get(
                f"{self.base_url}/api/organizers/{kind}/slug/{slug}",
                timeout=30,
            )
            if retry_existing.status_code == 200:
                data = retry_existing.json()
                return {"id": data["id"], "name": data["name"], "slug": data["slug"]}
            if create.status_code == 409:
                retry_existing.raise_for_status()

        create.raise_for_status()
        raise RuntimeError(f"Failed to create {kind[:-1]}: {name}")

    def create_recipe_stub(self, name: str) -> str:
        response = self.session.post(
            f"{self.base_url}/api/recipes",
            json={"name": name},
            timeout=30,
        )
        if response.status_code == 201:
            return response.text.strip().strip('"')
        if response.status_code != 409:
            response.raise_for_status()

        unique_name = f"{name} (mela-{int(time.time())})"
        retry = self.session.post(
            f"{self.base_url}/api/recipes",
            json={"name": unique_name},
            timeout=30,
        )
        retry.raise_for_status()
        return retry.text.strip().strip('"')

    def patch_recipe(self, slug: str, payload: dict) -> None:
        response = self.session.patch(
            f"{self.base_url}/api/recipes/{slug}",
            json=payload,
            timeout=60,
        )
        response.raise_for_status()

    def delete_recipe(self, slug: str) -> None:
        response = self.session.delete(f"{self.base_url}/api/recipes/{slug}", timeout=30)
        if response.status_code not in (200, 202, 204, 404):
            response.raise_for_status()

    def upload_image(self, slug: str, data: bytes, extension: str) -> None:
        with tempfile.NamedTemporaryFile(suffix=f".{extension}", delete=False) as tmp:
            tmp.write(data)
            tmp_path = Path(tmp.name)

        try:
            with tmp_path.open("rb") as handle:
                response = self.session.put(
                    f"{self.base_url}/api/recipes/{slug}/image",
                    files={"image": (f"recipe.{extension}", handle, f"image/{extension}")},
                    data={"extension": extension},
                    timeout=120,
                )
            response.raise_for_status()
        finally:
            tmp_path.unlink(missing_ok=True)

    def recipe_exists_by_slug(self, slug: str) -> bool:
        response = self.session.get(f"{self.base_url}/api/recipes/{slug}", timeout=30)
        if response.status_code == 200:
            return True
        if response.status_code == 404:
            return False
        response.raise_for_status()
        return False

    def recipe_slug_exists(self, slug: str) -> bool:
        return self.recipe_exists_by_slug(slug)


def print_dry_run_summary(entry: RecipeEntry, payload: dict) -> None:
    title = payload.get("name", "Untitled")
    categories = [category for category in (entry.recipe.get("categories") or []) if category]
    images = "yes" if entry.recipe.get("images") else "no"
    print(title)
    print(f"  Source file: {entry.archive_name}")
    print(f"  Categories: {', '.join(categories) if categories else '(none)'}")
    print(f"  Ingredients: {len(payload.get('recipeIngredient', []))}")
    print(f"  Steps: {len(payload.get('recipeInstructions', []))}")
    print(f"  Image: {images}")
    print()


def print_import_summary(export_path: Path, state_path: Path, log_path: Path) -> int:
    state = load_state(state_path, export_path)
    total = count_archive_entries(export_path)
    processed = state.get("processed", {})

    imported = 0
    skipped_existing = 0
    failed = 0
    other = 0

    for item in processed.values():
        status = item.get("status")
        if status == "imported":
            imported += 1
        elif status == "skipped_existing":
            skipped_existing += 1
        elif status == "failed":
            failed += 1
        else:
            other += 1

    remaining = max(total - imported - skipped_existing - failed, 0)

    log_counts: dict[str, int] = {}
    recent_failures: list[dict] = []
    for record in iter_log_records(log_path) or []:
        status = record.get("status", "unknown")
        log_counts[status] = log_counts.get(status, 0) + 1
        if status in {"failed", "cleanup_failed"}:
            recent_failures.append(record)

    print(f"Archive: {export_path.name}")
    print(f"State file: {state_path}")
    print(f"Log file: {log_path}")
    print(f"Total recipes: {total}")
    print(f"Imported: {imported}")
    print(f"Skipped existing: {skipped_existing}")
    print(f"Failed: {failed}")
    if other:
        print(f"Other tracked statuses: {other}")
    print(f"Remaining: {remaining}")
    print(f"Last scanned index: {state.get('last_scanned_index', -1)}")
    print(f"Tracked slug mappings: {len(state.get('slug_map', {}))}")

    if log_counts:
        print("Log event counts:")
        for status in sorted(log_counts):
            print(f"  {status}: {log_counts[status]}")

    if recent_failures:
        print("Recent failures:")
        for record in recent_failures[-5:]:
            title = record.get("title", "Untitled")
            status = record.get("status", "failed")
            error = record.get("error", "")
            print(f"  #{record.get('index', '?')} {status} {title}")
            if error:
                print(f"    {error}")

    return 0


def run_import(args: argparse.Namespace) -> int:
    export_path = Path(args.export).expanduser()
    if not export_path.exists():
        print(f"File not found: {export_path}")
        return 1

    state_path = Path(args.state_file).expanduser() if args.state_file else default_state_path(export_path)
    log_path = Path(args.log_file).expanduser() if args.log_file else default_log_path(export_path)

    if args.summary:
        return print_import_summary(export_path, state_path, log_path)

    if args.dry_run:
        dry_run_offset = args.offset if args.offset is not None else 0
        entries = load_selected_entries(export_path, args.limit, dry_run_offset)
        if not entries:
            print("No recipes selected.")
            return 1
        print(f"Selected {len(entries)} recipe(s) from {export_path.name}")
        for entry in entries:
            payload = build_recipe_payload(entry.recipe, {}, {})
            print_dry_run_summary(entry, payload)
        return 0

    if not args.url or not args.token:
        print("Live mode requires --url and --token.")
        return 1

    client = MealieClient(args.url, args.token)
    info = client.verify()
    print(f"Connected to Mealie {info.get('version', '?')} at {args.url}")

    state = load_state(state_path, export_path)

    batch_size = args.batch_size if args.batch_size is not None else args.limit
    if batch_size is None or batch_size <= 0:
        print("Batch size must be greater than zero.")
        return 1

    start_index = args.offset if args.offset is not None else max(state.get("last_scanned_index", -1) + 1, 0)
    total_successes = 0
    total_failures = 0
    created_slugs: list[str] = []
    batches_run = 0

    while True:
        entries = stream_entries(export_path, state, batch_size, start_index, args.retry_failed)
        save_state(state_path, state)
        if not entries:
            if batches_run == 0:
                if args.retry_failed:
                    print("No failed recipes to retry.")
                else:
                    print("No remaining recipes to import.")
            break

        batches_run += 1
        mode_label = "retry batch" if args.retry_failed else "batch"
        print(f"{mode_label.title()} {batches_run}: selected {len(entries)} recipe(s) starting from index {entries[0].index}")

        category_names = sorted(
            {
                category
                for entry in entries
                for category in (entry.recipe.get("categories") or [])
                if category
            }
        )

        tag_names = {"mela-import"}
        for entry in entries:
            if entry.recipe.get("favorite"):
                tag_names.add("favorite")
            if entry.recipe.get("wantToCook"):
                tag_names.add("want-to-cook")

        category_lookup = {
            slugify(name): client.get_or_create_organizer("categories", name)
            for name in category_names
        }
        tag_lookup = {
            slugify(name): client.get_or_create_organizer("tags", name)
            for name in sorted(tag_names)
        }

        batch_failures = 0

        for index, entry in enumerate(entries, start=1):
            title = entry.recipe.get("title") or "Untitled"
            identity = recipe_identity(entry)
            print(f"[{index}/{len(entries)}] #{entry.index} {title}")

            known_slug = recorded_slug(state, identity)
            if known_slug and client.recipe_slug_exists(known_slug):
                record_state(state, entry, "skipped_existing", slug=known_slug)
                save_state(state_path, state)
                append_log(
                    log_path,
                    {
                        "timestamp": now_utc_iso(),
                        "index": entry.index,
                        "identity": identity,
                        "title": title,
                        "status": "skipped_existing",
                        "slug": known_slug,
                        "reason": "known_state_mapping",
                    },
                )
                print(f"  Skipped existing recipe /recipe/{known_slug} (known mapping)")
                continue

            candidate_slug = slugify(title)
            if not known_slug and client.recipe_exists_by_slug(candidate_slug):
                record_state(state, entry, "skipped_existing", slug=candidate_slug)
                save_state(state_path, state)
                append_log(
                    log_path,
                    {
                        "timestamp": now_utc_iso(),
                        "index": entry.index,
                        "identity": identity,
                        "title": title,
                        "status": "skipped_existing",
                        "slug": candidate_slug,
                        "reason": "title_slug_exists",
                    },
                )
                print(f"  Skipped existing recipe /recipe/{candidate_slug}")
                continue

            created_slug: str | None = None
            try:
                created_slug = client.create_recipe_stub(title)
                payload = build_recipe_payload(entry.recipe, category_lookup, tag_lookup)
                client.patch_recipe(created_slug, payload)

                if not args.skip_images:
                    decoded = decode_first_image(entry.recipe)
                    if decoded:
                        image_data, extension = decoded
                        client.upload_image(created_slug, image_data, extension)

                created_slugs.append(created_slug)
                total_successes += 1
                record_state(state, entry, "imported", slug=created_slug)
                save_state(state_path, state)
                append_log(
                    log_path,
                    {
                        "timestamp": now_utc_iso(),
                        "index": entry.index,
                        "identity": identity,
                        "title": title,
                        "status": "imported",
                        "slug": created_slug,
                    },
                )
                print(f"  Imported as /recipe/{created_slug}")
            except requests.RequestException as exc:
                total_failures += 1
                batch_failures += 1
                error_text = str(exc)
                record_state(state, entry, "failed", slug=created_slug, error=error_text)
                save_state(state_path, state)
                append_log(
                    log_path,
                    {
                        "timestamp": now_utc_iso(),
                        "index": entry.index,
                        "identity": identity,
                        "title": title,
                        "status": "failed",
                        "slug": created_slug,
                        "error": error_text,
                    },
                )
                print(f"  Failed: {error_text}")
                if args.cleanup_on_failure and created_slug:
                    try:
                        client.delete_recipe(created_slug)
                        print("  Deleted incomplete stub")
                    except requests.RequestException as cleanup_exc:
                        cleanup_error = str(cleanup_exc)
                        append_log(
                            log_path,
                            {
                                "timestamp": now_utc_iso(),
                                "index": entry.index,
                                "identity": identity,
                                "title": title,
                                "status": "cleanup_failed",
                                "slug": created_slug,
                                "error": cleanup_error,
                            },
                        )
                        print(f"  Stub cleanup failed: {cleanup_error}")
                if args.stop_on_error:
                    print("Stopping on first error.")
                    print()
                    print(f"State file: {state_path}")
                    print(f"Log file: {log_path}")
                    return 2

            time.sleep(args.delay_seconds)

        if batch_failures and args.stop_after_batch_error:
            print("Stopping because this batch had failures.")
            break

        if args.max_batches is not None and batches_run >= args.max_batches:
            break

        start_index = entries[-1].index + 1

    print()
    print(f"Imported: {total_successes}")
    print(f"Failed: {total_failures}")
    print(f"State file: {state_path}")
    print(f"Log file: {log_path}")
    if created_slugs:
        print("Created slugs:")
        for slug in created_slugs:
            print(f"  {slug}")
    return 0 if total_failures == 0 else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Safely import Mela recipes into Mealie",
    )
    parser.add_argument("export", help="Path to .melarecipes or .melarecipe")
    parser.add_argument("--url", help="Mealie base URL")
    parser.add_argument("--token", help="Mealie API token")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview only; no API calls are made",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Show import progress summary from the state and log files",
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Retry only recipes currently marked failed in the state file",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=2,
        help="Dry-run selection size, or live batch size when --batch-size is not set (default: 2)",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=None,
        help="Override the resume position and start scanning from this recipe index",
    )
    parser.add_argument(
        "--skip-images",
        action="store_true",
        help="Do not upload images",
    )
    parser.add_argument(
        "--cleanup-on-failure",
        action="store_true",
        help="Attempt to delete a created stub recipe if patching fails",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        help="Number of recipes to process per batch in live mode",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        help="Maximum number of batches to run in this invocation",
    )
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=0.3,
        help="Delay between recipe imports (default: 0.3)",
    )
    parser.add_argument(
        "--state-file",
        help="Path to the persistent import state JSON file",
    )
    parser.add_argument(
        "--log-file",
        help="Path to the append-only import log JSONL file",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop immediately when a recipe fails",
    )
    parser.add_argument(
        "--stop-after-batch-error",
        action="store_true",
        help="Finish the current batch, then stop if any recipe in that batch failed",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return run_import(args)


if __name__ == "__main__":
    raise SystemExit(main())
