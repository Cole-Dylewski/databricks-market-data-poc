#!/usr/bin/env python3
"""
CI/CD Monitor Script
Pushes changes to git and monitors GitHub Actions workflow runs.
"""

import os
import sys
import time
import subprocess
import json
from typing import Optional, Dict, Any
from datetime import datetime
from pathlib import Path

try:
    import requests
except ImportError:
    print("Error: requests library not found. Install it with: pip install requests")
    sys.exit(1)

try:
    from dotenv import load_dotenv
    HAS_DOTENV = True
except ImportError:
    HAS_DOTENV = False
    def load_dotenv(*args, **kwargs):
        pass


class CIMonitor:
    """Monitor GitHub Actions CI/CD workflows."""
    
    GITHUB_API_BASE = "https://api.github.com"
    POLL_INTERVAL = 5  # seconds
    MAX_WAIT_TIME = 1800  # 30 minutes
    ENV_FILE = Path(".env")
    
    def __init__(self, token: Optional[str] = None, repo: Optional[str] = None, save_token: bool = False):
        """Initialize the CI monitor.
        
        Args:
            token: GitHub personal access token (takes precedence over env/.env)
            repo: Repository in format 'owner/repo' (auto-detected from git if not provided)
            save_token: If True and token is provided, save it to .env file
        """
        # Load .env file if it exists
        if self.ENV_FILE.exists():
            if HAS_DOTENV:
                load_dotenv(dotenv_path=self.ENV_FILE)
            else:
                # Fallback: manually read .env file
                try:
                    with open(self.ENV_FILE, 'r', encoding='utf-8') as f:
                        for line in f:
                            line = line.strip()
                            if line and not line.startswith('#') and '=' in line:
                                key, value = line.split('=', 1)
                                key = key.strip()
                                value = value.strip()
                                if key == 'GITHUB_TOKEN':
                                    os.environ['GITHUB_TOKEN'] = value
                except Exception:
                    pass
        
        # Priority: provided token > environment variable > .env file
        if token:
            self.token = token
            if save_token:
                self._save_token_to_env(token)
        else:
            self.token = os.getenv("GITHUB_TOKEN")
        
        if not self.token:
            print("Warning: No GitHub token found.")
            print("You can:")
            print("  1. Pass --token argument")
            print("  2. Set GITHUB_TOKEN environment variable")
            print("  3. Add GITHUB_TOKEN to .env file")
            print("\nCreate a token at: https://github.com/settings/tokens")
            print("Required scopes: repo, actions:read")
            response = input("\nContinue without token? (y/n): ")
            if response.lower() != 'y':
                sys.exit(1)
        
        self.repo = repo or self._get_repo_from_git()
        if not self.repo:
            print("Error: Could not determine repository. Please provide repo as 'owner/repo'")
            sys.exit(1)
        
        self.headers = {
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"token {self.token}" if self.token else None
        }
        if not self.token:
            self.headers.pop("Authorization")
    
    def _save_token_to_env(self, token: str):
        """Save token to .env file.
        
        Args:
            token: GitHub token to save
        """
        try:
            # Read existing .env file if it exists
            env_vars = {}
            if self.ENV_FILE.exists():
                with open(self.ENV_FILE, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#') and '=' in line:
                            key, value = line.split('=', 1)
                            env_vars[key.strip()] = value.strip()
            
            # Update or add GITHUB_TOKEN
            env_vars['GITHUB_TOKEN'] = token
            
            # Write back to .env file
            with open(self.ENV_FILE, 'w', encoding='utf-8') as f:
                for key, value in env_vars.items():
                    f.write(f"{key}={value}\n")
            
            # Add .env to .gitignore if not already there
            gitignore_path = Path(".gitignore")
            if gitignore_path.exists():
                with open(gitignore_path, 'r', encoding='utf-8') as f:
                    gitignore_content = f.read()
                if '.env' not in gitignore_content:
                    with open(gitignore_path, 'a', encoding='utf-8') as f:
                        f.write("\n# GitHub token\n.env\n")
            else:
                with open(gitignore_path, 'w', encoding='utf-8') as f:
                    f.write("# GitHub token\n.env\n")
            
            print(f"[OK] Token saved to {self.ENV_FILE}")
            print(f"[OK] Added .env to .gitignore")
        except Exception as e:
            print(f"Warning: Could not save token to .env file: {e}")
    
    def _get_repo_from_git(self) -> Optional[str]:
        """Get repository owner/name from git remote."""
        try:
            result = subprocess.run(
                ["git", "remote", "get-url", "origin"],
                capture_output=True,
                text=True,
                check=True
            )
            url = result.stdout.strip()
            
            # Handle both HTTPS and SSH URLs
            if "github.com" in url:
                if url.startswith("https://"):
                    # https://github.com/owner/repo.git
                    parts = url.replace("https://github.com/", "").replace(".git", "").split("/")
                elif url.startswith("git@"):
                    # git@github.com:owner/repo.git
                    parts = url.split(":")[1].replace(".git", "").split("/")
                else:
                    return None
                
                if len(parts) >= 2:
                    return f"{parts[0]}/{parts[1]}"
        except (subprocess.CalledProcessError, Exception) as e:
            print(f"Warning: Could not get repo from git: {e}")
        
        return None
    
    def _run_git_command(self, command: list[str]) -> tuple[bool, str]:
        """Run a git command and return success status and output."""
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=True
            )
            return True, result.stdout.strip()
        except subprocess.CalledProcessError as e:
            return False, e.stderr.strip()
    
    def push_changes(self, branch: Optional[str] = None, commit_message: Optional[str] = None) -> bool:
        """Push changes to the remote repository.
        
        Args:
            branch: Branch to push to (default: current branch)
            commit_message: Commit message if there are uncommitted changes
        
        Returns:
            True if push was successful
        """
        # Check if there are uncommitted changes
        success, status = self._run_git_command(["git", "status", "--porcelain"])
        if not success:
            print(f"Error checking git status: {status}")
            return False
        
        if status:
            print("Uncommitted changes detected.")
            if commit_message:
                # Stage all changes
                success, _ = self._run_git_command(["git", "add", "-A"])
                if not success:
                    print("Error staging changes")
                    return False
                
                # Commit
                success, output = self._run_git_command(["git", "commit", "-m", commit_message])
                if not success:
                    print(f"Error committing: {output}")
                    return False
                print(f"Committed changes: {commit_message}")
            else:
                print("No commit message provided. Skipping commit.")
                return False
        
        # Get current branch if not provided
        if not branch:
            success, branch = self._run_git_command(["git", "rev-parse", "--abbrev-ref", "HEAD"])
            if not success:
                print(f"Error getting current branch: {branch}")
                return False
        
        # Push
        print(f"Pushing to {branch}...")
        success, output = self._run_git_command(["git", "push", "origin", branch])
        if success:
            print(f"[OK] Successfully pushed to {branch}")
            return True
        else:
            print(f"[ERROR] Error pushing: {output}")
            return False
    
    def get_latest_workflow_run(self, workflow_name: str = "CI") -> Optional[Dict[str, Any]]:
        """Get the latest workflow run for the specified workflow.
        
        Args:
            workflow_name: Name of the workflow (default: "CI")
        
        Returns:
            Workflow run data or None if not found
        """
        if not self.token:
            print("Warning: No token available. Cannot fetch workflow runs via API.")
            return None
        
        # First, get workflow ID by name
        url = f"{self.GITHUB_API_BASE}/repos/{self.repo}/actions/workflows"
        response = requests.get(url, headers=self.headers)
        
        if response.status_code != 200:
            print(f"Error fetching workflows: {response.status_code} - {response.text}")
            return None
        
        workflows = response.json().get("workflows", [])
        workflow = next((w for w in workflows if w["name"] == workflow_name), None)
        
        if not workflow:
            print(f"Workflow '{workflow_name}' not found")
            return None
        
        # Get latest run for this workflow
        url = f"{self.GITHUB_API_BASE}/repos/{self.repo}/actions/workflows/{workflow['id']}/runs"
        params = {"per_page": 1}
        response = requests.get(url, headers=self.headers, params=params)
        
        if response.status_code != 200:
            print(f"Error fetching workflow runs: {response.status_code} - {response.text}")
            return None
        
        runs = response.json().get("workflow_runs", [])
        return runs[0] if runs else None
    
    def get_workflow_run_status(self, run_id: int) -> Dict[str, Any]:
        """Get status of a specific workflow run.
        
        Args:
            run_id: Workflow run ID
        
        Returns:
            Workflow run status data
        """
        if not self.token:
            return {}
        
        url = f"{self.GITHUB_API_BASE}/repos/{self.repo}/actions/runs/{run_id}"
        response = requests.get(url, headers=self.headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching run status: {response.status_code}")
            return {}
    
    def get_job_logs(self, job_id: int) -> Optional[str]:
        """Get logs for a specific job.
        
        Args:
            job_id: Job ID
        
        Returns:
            Log content or None
        """
        if not self.token:
            return None
        
        url = f"{self.GITHUB_API_BASE}/repos/{self.repo}/actions/jobs/{job_id}/logs"
        response = requests.get(url, headers=self.headers)
        
        if response.status_code == 200:
            return response.text
        else:
            print(f"Error fetching job logs: {response.status_code}")
            return None
    
    def get_run_jobs(self, run_id: int) -> list[Dict[str, Any]]:
        """Get all jobs for a workflow run.
        
        Args:
            run_id: Workflow run ID
        
        Returns:
            List of job data
        """
        if not self.token:
            return []
        
        url = f"{self.GITHUB_API_BASE}/repos/{self.repo}/actions/runs/{run_id}/jobs"
        response = requests.get(url, headers=self.headers)
        
        if response.status_code == 200:
            return response.json().get("jobs", [])
        else:
            print(f"Error fetching jobs: {response.status_code}")
            return []
    
    def wait_for_completion(self, run_id: int) -> Dict[str, Any]:
        """Wait for workflow run to complete and return final status.
        
        Args:
            run_id: Workflow run ID
        
        Returns:
            Final workflow run status
        """
        start_time = time.time()
        print(f"\nMonitoring workflow run {run_id}...")
        print("=" * 60)
        
        while True:
            status_data = self.get_workflow_run_status(run_id)
            if not status_data:
                print("Error: Could not fetch workflow status")
                break
            
            status = status_data.get("status", "unknown")
            conclusion = status_data.get("conclusion")
            
            # Display current status
            elapsed = int(time.time() - start_time)
            status_str = status.upper()
            if conclusion:
                status_str += f" ({conclusion.upper()})"
            
            print(f"[{elapsed}s] Status: {status_str}", end="\r")
            
            # Check if completed
            if status == "completed":
                print(f"\n{'=' * 60}")
                return status_data
            
            # Check timeout
            if time.time() - start_time > self.MAX_WAIT_TIME:
                print(f"\nTimeout: Workflow did not complete within {self.MAX_WAIT_TIME}s")
                break
            
            time.sleep(self.POLL_INTERVAL)
        
        return status_data
    
    def display_results(self, run_data: Dict[str, Any]):
        """Display workflow run results and logs.
        
        Args:
            run_data: Workflow run data
        """
        if not run_data:
            print("No workflow run data available")
            return
        
        run_id = run_data.get("id")
        status = run_data.get("status", "unknown")
        conclusion = run_data.get("conclusion", "unknown")
        html_url = run_data.get("html_url", "")
        
        print(f"\n{'=' * 60}")
        print("WORKFLOW RUN RESULTS")
        print(f"{'=' * 60}")
        print(f"Run ID: {run_id}")
        print(f"Status: {status}")
        print(f"Conclusion: {conclusion}")
        print(f"URL: {html_url}")
        print(f"{'=' * 60}\n")
        
        if not self.token:
            print("No token available. Cannot fetch detailed job information.")
            print(f"View details at: {html_url}")
            return
        
        # Get jobs
        jobs = self.get_run_jobs(run_id)
        if not jobs:
            print("No jobs found for this run")
            return
        
        print(f"Jobs ({len(jobs)}):")
        print("-" * 60)
        
        for job in jobs:
            job_name = job.get("name", "Unknown")
            job_status = job.get("status", "unknown")
            job_conclusion = job.get("conclusion", "unknown")
            job_id = job.get("id")
            
            # Status indicator
            if job_conclusion == "success":
                indicator = "[OK]"
            elif job_conclusion == "failure":
                indicator = "[FAIL]"
            else:
                indicator = "[?]"
            
            print(f"\n{indicator} {job_name}")
            print(f"   Status: {job_status} | Conclusion: {job_conclusion}")
            
            # Get and display logs if failed
            if job_conclusion == "failure" and job_id:
                print(f"   Fetching logs...")
                logs = self.get_job_logs(job_id)
                if logs:
                    # Show last 50 lines of logs
                    log_lines = logs.split("\n")
                    print(f"   Last 50 lines of logs:")
                    print("   " + "-" * 56)
                    for line in log_lines[-50:]:
                        print(f"   {line}")
                    print("   " + "-" * 56)
        
        print(f"\n{'=' * 60}")
        print(f"View full details: {html_url}")
        print(f"{'=' * 60}\n")
    
    def run(self, commit_message: Optional[str] = None, branch: Optional[str] = None):
        """Main execution: push changes and monitor workflow.
        
        Args:
            commit_message: Commit message for uncommitted changes
            branch: Branch to push to
        """
        print("CI/CD Monitor")
        print("=" * 60)
        print(f"Repository: {self.repo}")
        print(f"{'=' * 60}\n")
        
        # Push changes
        if not self.push_changes(branch=branch, commit_message=commit_message):
            print("Failed to push changes. Exiting.")
            return
        
        # Wait a moment for GitHub to register the push
        print("\nWaiting for workflow to start...")
        time.sleep(3)
        
        # Get latest workflow run
        run_data = self.get_latest_workflow_run()
        if not run_data:
            print("Could not find workflow run. It may take a moment to appear.")
            print(f"Check manually at: https://github.com/{self.repo}/actions")
            return
        
        run_id = run_data.get("id")
        print(f"Found workflow run: {run_id}")
        
        # Wait for completion
        final_status = self.wait_for_completion(run_id)
        
        # Display results
        self.display_results(final_status)
        
        # Exit with appropriate code
        conclusion = final_status.get("conclusion", "unknown")
        if conclusion == "success":
            print("[SUCCESS] Workflow completed successfully!")
            sys.exit(0)
        elif conclusion == "failure":
            print("[FAILURE] Workflow failed!")
            sys.exit(1)
        else:
            print(f"[UNKNOWN] Workflow completed with status: {conclusion}")
            sys.exit(0)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Push changes and monitor GitHub Actions CI/CD workflows"
    )
    parser.add_argument(
        "--token",
        help="GitHub personal access token (saved to .env if --save-token is used)",
        default=None
    )
    parser.add_argument(
        "--save-token",
        action="store_true",
        help="Save provided token to .env file (use with --token)"
    )
    parser.add_argument(
        "--repo",
        help="Repository in format 'owner/repo' (auto-detected from git if not provided)",
        default=None
    )
    parser.add_argument(
        "--branch",
        help="Branch to push to (default: current branch)",
        default=None
    )
    parser.add_argument(
        "--commit-message",
        "-m",
        help="Commit message for uncommitted changes",
        default=None
    )
    
    args = parser.parse_args()
    
    # Validate save-token usage
    if args.save_token and not args.token:
        print("Error: --save-token requires --token to be provided")
        sys.exit(1)
    
    monitor = CIMonitor(
        token=args.token,
        repo=args.repo,
        save_token=args.save_token
    )
    monitor.run(commit_message=args.commit_message, branch=args.branch)


if __name__ == "__main__":
    main()

