# CI/CD Monitor Script

A Python script that pushes changes to git and automatically monitors GitHub Actions workflow runs, displaying status and logs when complete.

## Features

- ✅ Automatically pushes changes to git
- ✅ Detects repository from git remote
- ✅ Monitors workflow runs in real-time
- ✅ Displays job status and conclusions
- ✅ Shows logs for failed jobs
- ✅ Exits with appropriate status codes

## Setup

### 1. Install Dependencies

The script requires the `requests` library:

```bash
pip install requests
```

### 2. GitHub Token (Optional but Recommended)

For full functionality (fetching workflow status and logs), you need a GitHub personal access token:

1. Go to: https://github.com/settings/tokens
2. Click "Generate new token (classic)"
3. Select scopes:
   - `repo` (full control of private repositories)
   - `actions:read` (read access to actions)
4. Copy the token

**Three ways to provide the token:**

**Option 1: Save to .env file (Recommended)**
```bash
python ci_monitor.py --token your_token_here --save-token
```
This saves the token to `.env` file (automatically added to `.gitignore`). The token will be loaded automatically on future runs.

**Option 2: Environment variable**
```bash
# Windows (PowerShell)
$env:GITHUB_TOKEN = "your_token_here"

# Windows (Command Prompt)
set GITHUB_TOKEN=your_token_here

# Linux/Mac
export GITHUB_TOKEN=your_token_here
```

**Option 3: Pass as argument each time**
```bash
python ci_monitor.py --token your_token_here
```

**Note:** The script will work without a token, but you'll only see basic information and won't be able to fetch detailed logs.

## Usage

### Basic Usage

Simply run the script - it will:
1. Check for uncommitted changes
2. Push to the current branch
3. Monitor the latest workflow run
4. Display results

```bash
python ci_monitor.py
```

### With Uncommitted Changes

If you have uncommitted changes, provide a commit message:

```bash
python ci_monitor.py -m "Your commit message"
```

### Specify Branch

Push to a specific branch:

```bash
python ci_monitor.py --branch develop
```

### Save Token to .env File (One-time setup)

Save your token to `.env` file for automatic loading on future runs:

```bash
python ci_monitor.py --token your_token_here --save-token
```

After this, you can run the script without the token argument - it will load from `.env` automatically.

### With Token (One-time use)

Pass token as argument without saving:

```bash
python ci_monitor.py --token your_token_here
```

### Specify Repository

If auto-detection fails:

```bash
python ci_monitor.py --repo owner/repo-name
```

### Full Example

```bash
# First time: save token
python ci_monitor.py --token your_token_here --save-token

# Future runs: token loaded from .env automatically
python ci_monitor.py --branch main -m "Update CI workflow"
```

## Output

The script will:

1. **Push changes** and confirm success
2. **Wait for workflow to start** (3 second delay)
3. **Monitor in real-time** showing elapsed time and status
4. **Display results** when complete:
   - Overall workflow status
   - Individual job status (✓/✗)
   - Logs for failed jobs (last 50 lines)
   - Direct link to GitHub Actions page

### Example Output

```
CI/CD Monitor
============================================================
Repository: Cole-Dylewski/market_data_poc
============================================================

Pushing to main...
✓ Successfully pushed to main

Waiting for workflow to start...
Found workflow run: 123456789

Monitoring workflow run 123456789...
============================================================
[45s] Status: IN_PROGRESS
============================================================

============================================================
WORKFLOW RUN RESULTS
============================================================
Run ID: 123456789
Status: completed
Conclusion: success
URL: https://github.com/Cole-Dylewski/market_data_poc/actions/runs/123456789
============================================================

Jobs (2):
------------------------------------------------------------

✓ test
   Status: completed | Conclusion: success

✓ lint
   Status: completed | Conclusion: success

============================================================
View full details: https://github.com/Cole-Dylewski/market_data_poc/actions/runs/123456789
============================================================

✓ Workflow completed successfully!
```

## Exit Codes

- `0`: Workflow completed successfully
- `1`: Workflow failed
- Other: Workflow completed with unknown status

This makes it easy to use in scripts:

```bash
python ci_monitor.py && echo "CI passed!" || echo "CI failed!"
```

## Troubleshooting

### "No GitHub token found"

The script will work without a token, but you'll only see basic information. Set `GITHUB_TOKEN` environment variable or use `--token` flag.

### "Could not determine repository"

Make sure you're in a git repository with a remote named `origin` pointing to GitHub. Or specify the repo manually with `--repo owner/repo`.

### "Workflow not found"

Make sure your workflow file (`.github/workflows/ci.yml`) exists and is named "CI" (or specify the correct name in the script).

### Script hangs

The script will timeout after 30 minutes. If a workflow takes longer, you can modify `MAX_WAIT_TIME` in the script.

## Integration

You can integrate this into your workflow:

```bash
# In a Makefile
ci:
	python ci_monitor.py -m "Automated CI check"

# In a shell script
#!/bin/bash
python ci_monitor.py "$@" || exit 1
```

