# Mela to Mealie Importer

Import recipes from a Mela `.melarecipes` or `.melarecipe` export into Mealie using the Mealie API.

The importer is designed to be safe for large, real-world migrations:

- imports in batches
- resumes from a local state file
- skips already imported recipes
- records a per-recipe log
- supports retrying only failed recipes
- uploads images to Mealie so Mealie handles its own image processing

## Features

- Reads Mela bulk exports (`.melarecipes`) and single recipe exports (`.melarecipe`)
- Converts ingredients, instructions, categories, notes, nutrition, and times
- Creates missing Mealie categories and tags
- Uploads the first embedded Mela image as the Mealie recipe image
- Tracks imported recipes by Mela recipe ID (or archive filename fallback)
- Writes resumable state and append-only logs
- Supports `--dry-run`, `--summary`, `--retry-failed`, integrated ingredient repair, and integrated author repair

## Requirements

- Python 3.10+
- `requests`
- A reachable Mealie instance with an API token

Install the dependency:

```bash
pip install -r requirements.txt
```

## Quick Start

1. Export your recipes from Mela as `Recipes.melarecipes`.
2. Create a Mealie backup and API token.
3. Run a dry run:

```bash
python3 mela_to_mealie_import.py Recipes.melarecipes --dry-run
```

4. Run one live batch of 100:

```bash
python3 mela_to_mealie_import.py Recipes.melarecipes \
  --url http://your-mealie-host:9925 \
  --token YOUR_API_TOKEN \
  --batch-size 100 \
  --max-batches 1 \
  --cleanup-on-failure \
  --stop-after-batch-error
```

5. Check progress:

```bash
python3 mela_to_mealie_import.py Recipes.melarecipes --summary
```

6. Re-run the same live command without `--max-batches 1` to continue.

7. If needed, repair any Mealie placeholder ingredients:

```bash
python3 mela_to_mealie_import.py Recipes.melarecipes \
  --url http://your-mealie-host:9925 \
  --token YOUR_API_TOKEN \
  --repair-placeholder-ingredients \
  --placeholder-only
```

8. If needed, infer and store source authors from recipe titles:

```bash
python3 mela_to_mealie_import.py Recipes.melarecipes \
  --url http://your-mealie-host:9925 \
  --token YOUR_API_TOKEN \
  --repair-authors
```

## Files

- `mela_to_mealie_import.py`: importer script
- `requirements.txt`: Python dependencies
- `Recipes.melarecipes.import-state.json`: generated resumable state file
- `Recipes.melarecipes.import-log.jsonl`: generated import log

The state and log files are created automatically during live imports.
You can override their locations with `--state-file` and `--log-file`.

## Mealie Preparation

1. Create a full Mealie backup before importing.
2. Create a dedicated API token in Mealie.
3. Use a small test batch first.

## Usage

### Dry run

Preview a couple of recipes without calling Mealie:

```bash
python3 mela_to_mealie_import.py Recipes.melarecipes --dry-run
```

Preview a different slice:

```bash
python3 mela_to_mealie_import.py Recipes.melarecipes --dry-run --limit 5 --offset 25
```

### Run one live batch

This imports one batch of 100 recipes, then stops:

```bash
python3 mela_to_mealie_import.py Recipes.melarecipes \
  --url http://your-mealie-host:9925 \
  --token YOUR_API_TOKEN \
  --batch-size 100 \
  --max-batches 1 \
  --cleanup-on-failure \
  --stop-after-batch-error
```

### Resume import

Run the same command again without `--max-batches 1` to continue automatically from the saved state:

```bash
python3 mela_to_mealie_import.py Recipes.melarecipes \
  --url http://your-mealie-host:9925 \
  --token YOUR_API_TOKEN \
  --batch-size 100 \
  --cleanup-on-failure \
  --stop-after-batch-error
```

### Show progress summary

```bash
python3 mela_to_mealie_import.py Recipes.melarecipes --summary
```

### Retry only failed recipes

```bash
python3 mela_to_mealie_import.py Recipes.melarecipes \
  --url http://your-mealie-host:9925 \
  --token YOUR_API_TOKEN \
  --batch-size 25 \
  --retry-failed \
  --cleanup-on-failure \
  --stop-after-batch-error
```

### Repair placeholder ingredients

If a small number of recipes end up with Mealie's placeholder ingredient row (`Could not detect ingredients`), the importer can repair them from the original Mela ingredient lines:

```bash
python3 mela_to_mealie_import.py Recipes.melarecipes \
  --url http://your-mealie-host:9925 \
  --token YOUR_API_TOKEN \
  --repair-placeholder-ingredients \
  --placeholder-only
```

Preview only:

```bash
python3 mela_to_mealie_import.py Recipes.melarecipes \
  --url http://your-mealie-host:9925 \
  --token YOUR_API_TOKEN \
  --repair-placeholder-ingredients \
  --placeholder-only \
  --dry-run
```

### Repair source authors

If you want visible source-author attribution in Mealie, the importer can infer authors from common title patterns and store them in both `extras.sourceAuthor` and a visible `Source Author` note.

```bash
python3 mela_to_mealie_import.py Recipes.melarecipes \
  --url http://your-mealie-host:9925 \
  --token YOUR_API_TOKEN \
  --repair-authors
```

Only fill missing author values:

```bash
python3 mela_to_mealie_import.py Recipes.melarecipes \
  --url http://your-mealie-host:9925 \
  --token YOUR_API_TOKEN \
  --repair-authors \
  --author-only-missing
```

Preview only:

```bash
python3 mela_to_mealie_import.py Recipes.melarecipes \
  --url http://your-mealie-host:9925 \
  --token YOUR_API_TOKEN \
  --repair-authors \
  --dry-run \
  --limit 10
```

## How resume and duplicate protection work

- Each recipe is tracked in the state file using the Mela recipe `id` when present.
- The state file stores the Mealie slug created for that recipe.
- Re-running the importer skips recipes already marked as imported or already known to exist.
- A title-based slug check is only used as a fallback when no prior mapping exists.

This makes it safe to stop and rerun the importer without manually tracking offsets.

## Notes

- The importer uploads only the first image in each Mela recipe because Mealie uses a single primary recipe image.
- The importer writes local state and log files in the working directory by default. These should not be committed.
- For very large exports, `--summary` scans the archive to calculate a true remaining count, so it can take a little time.
- Ingredient repair uses the original Mela ingredient lines and patches them back into Mealie as plain rows, which is compatible with Mealie v3.11.0.
- Author repair is best-effort because Mela does not export a dedicated author field; the importer infers author names from recipe titles and stores them separately from the recipe title.
- The project is licensed under the MIT License. See `LICENSE`.
