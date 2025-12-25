#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
MAIN_BRANCH="main"
MAX_WAIT_SECONDS=300  # 5 minutes
POLL_INTERVAL=10

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Check prerequisites
check_prerequisites() {
    if ! command -v gh &> /dev/null; then
        log_error "GitHub CLI (gh) is required. Install with: brew install gh"
        exit 1
    fi

    if ! command -v doctl &> /dev/null; then
        log_error "DigitalOcean CLI (doctl) is required. Install with: brew install doctl"
        exit 1
    fi

    if ! gh auth status &> /dev/null; then
        log_error "GitHub CLI not authenticated. Run: gh auth login"
        exit 1
    fi
}

# Get current branch and check for uncommitted changes
check_git_status() {
    if [[ -n $(git status --porcelain) ]]; then
        log_error "You have uncommitted changes. Please commit or stash them first."
        exit 1
    fi
}

# Create release branch
create_release_branch() {
    local timestamp=$(date +%Y%m%d-%H%M%S)
    RELEASE_BRANCH="release/${timestamp}"

    log_info "Creating release branch: ${RELEASE_BRANCH}"
    git checkout -b "${RELEASE_BRANCH}"
    git push -u origin "${RELEASE_BRANCH}"
}

# Create PR
create_pr() {
    log_info "Creating pull request..."

    PR_URL=$(gh pr create \
        --base "${MAIN_BRANCH}" \
        --head "${RELEASE_BRANCH}" \
        --title "Release $(date +%Y-%m-%d)" \
        --body "Automated release PR

## Changes
$(git log ${MAIN_BRANCH}..HEAD --oneline)

---
This PR will be auto-merged after tests pass.")

    PR_NUMBER=$(echo "${PR_URL}" | grep -oE '[0-9]+$')
    log_info "Created PR #${PR_NUMBER}: ${PR_URL}"
}

# Wait for CI checks to complete
wait_for_checks() {
    log_info "Waiting for CI checks to complete..."

    local elapsed=0
    while [ $elapsed -lt $MAX_WAIT_SECONDS ]; do
        # Get check status
        local status=$(gh pr checks "${PR_NUMBER}" --json state,name 2>/dev/null || echo "pending")

        if echo "$status" | grep -q '"state":"SUCCESS"'; then
            log_info "All checks passed!"
            return 0
        elif echo "$status" | grep -q '"state":"FAILURE"'; then
            log_error "CI checks failed!"
            gh pr checks "${PR_NUMBER}"
            return 1
        elif echo "$status" | grep -q '"state":"ERROR"'; then
            log_error "CI checks errored!"
            gh pr checks "${PR_NUMBER}"
            return 1
        fi

        log_info "Checks still running... (${elapsed}s elapsed)"
        sleep $POLL_INTERVAL
        elapsed=$((elapsed + POLL_INTERVAL))
    done

    log_error "Timeout waiting for CI checks (${MAX_WAIT_SECONDS}s)"
    return 1
}

# Merge PR
merge_pr() {
    log_info "Merging PR #${PR_NUMBER}..."
    gh pr merge "${PR_NUMBER}" --squash --delete-branch
    log_info "PR merged successfully!"
}

# Deploy to DigitalOcean
deploy() {
    log_info "Deploying to DigitalOcean Functions..."

    # Switch back to main and pull latest
    git checkout "${MAIN_BRANCH}"
    git pull origin "${MAIN_BRANCH}"

    # Deploy
    doctl serverless deploy .

    log_info "Deployment complete!"

    # Show deployed version
    doctl serverless functions get airquality/check-alerts | grep -E '"version"'
}

# Cleanup on failure
cleanup() {
    if [ -n "${RELEASE_BRANCH}" ]; then
        log_warn "Cleaning up release branch..."
        git checkout "${MAIN_BRANCH}" 2>/dev/null || true
        git branch -D "${RELEASE_BRANCH}" 2>/dev/null || true
        git push origin --delete "${RELEASE_BRANCH}" 2>/dev/null || true
    fi
}

# Main
main() {
    log_info "Starting publish process..."

    check_prerequisites
    check_git_status

    # Ensure we're on main
    current_branch=$(git rev-parse --abbrev-ref HEAD)
    if [ "${current_branch}" != "${MAIN_BRANCH}" ]; then
        log_error "Must be on ${MAIN_BRANCH} branch. Currently on: ${current_branch}"
        exit 1
    fi

    # Pull latest
    log_info "Pulling latest from ${MAIN_BRANCH}..."
    git pull origin "${MAIN_BRANCH}"

    # Run tests locally first
    log_info "Running tests locally..."
    cd packages/airquality/check-alerts
    python -m pytest test_aqi.py -v --tb=short
    cd - > /dev/null

    # Create branch and PR
    create_release_branch
    create_pr

    # Wait for CI
    if ! wait_for_checks; then
        cleanup
        exit 1
    fi

    # Merge and deploy
    merge_pr
    deploy

    log_info "Publish complete!"
}

# Run with cleanup on error
trap cleanup ERR
main "$@"
