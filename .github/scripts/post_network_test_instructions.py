#!/usr/bin/env python3
"""
Post network test instructions to PR comments.
"""

import os
import sys
import requests
import json
from typing import Optional, Dict, Any


def get_github_context() -> Dict[str, Any]:
    """Get GitHub context from environment variables."""
    github_token = os.getenv('GITHUB_TOKEN')
    github_repository = os.getenv('GITHUB_REPOSITORY')
    github_event_path = os.getenv('GITHUB_EVENT_PATH')
    
    if not all([github_token, github_repository, github_event_path]):
        raise ValueError("Missing required GitHub environment variables")
    
    with open(github_event_path, 'r') as f:
        event_data = json.load(f)
    
    return {
        'token': github_token,
        'repository': github_repository,
        'event': event_data
    }


def get_pr_number(event_data: Dict[str, Any]) -> int:
    """Extract PR number from GitHub event data."""
    if 'pull_request' in event_data:
        return event_data['pull_request']['number']
    elif 'number' in event_data:
        return event_data['number']
    else:
        raise ValueError("Unable to determine PR number from event data")


def create_comment_body(pr_number: int) -> str:
    """Create the comment body with network test instructions."""
    return f"""## 🧪 Network Tests

To run network tests for this PR, use:
```bash
gh workflow run network-tests.yml -f pr_number={pr_number}
```

**Available test options:**
- Run all tests: `gh workflow run network-tests.yml -f pr_number={pr_number}`
- Run specific test: `gh workflow run network-tests.yml -f pr_number={pr_number} -f test_selection=ping-pong`

**Test types:** `destroyable`, `ping-pong`, `one-to-many-internal-messages`, `fq-deploy`, `nft-index`

Results will be posted as workflow runs in the Actions tab."""


def find_existing_comment(github_token: str, repo: str, pr_number: int) -> Optional[int]:
    """Find existing network test instructions comment."""
    url = f"https://api.github.com/repos/{repo}/issues/{pr_number}/comments"
    headers = {
        'Authorization': f'token {github_token}',
        'Accept': 'application/vnd.github.v3+json'
    }
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    comments = response.json()
    
    for comment in comments:
        if (comment.get('user', {}).get('type') == 'Bot' and 
            '🧪 Network Tests' in comment.get('body', '')):
            return comment['id']
    
    return None


def post_or_update_comment(github_token: str, repo: str, pr_number: int, body: str) -> None:
    """Post new comment or update existing one."""
    headers = {
        'Authorization': f'token {github_token}',
        'Accept': 'application/vnd.github.v3+json'
    }
    
    existing_comment_id = find_existing_comment(github_token, repo, pr_number)
    
    if existing_comment_id:
        # Update existing comment
        url = f"https://api.github.com/repos/{repo}/issues/comments/{existing_comment_id}"
        data = {'body': body}
        response = requests.patch(url, headers=headers, json=data)
        print(f"Updated existing comment {existing_comment_id}")
    else:
        # Create new comment
        url = f"https://api.github.com/repos/{repo}/issues/{pr_number}/comments"
        data = {'body': body}
        response = requests.post(url, headers=headers, json=data)
        print(f"Created new comment on PR {pr_number}")
    
    response.raise_for_status()


def main():
    """Main function."""
    try:
        context = get_github_context()
        pr_number = get_pr_number(context['event'])
        
        print(f"Processing PR #{pr_number}")
        
        comment_body = create_comment_body(pr_number)
        post_or_update_comment(
            context['token'],
            context['repository'],
            pr_number,
            comment_body
        )
        
        print("Successfully posted network test instructions")
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()