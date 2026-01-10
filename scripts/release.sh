#!/bin/bash
set -e

# =============================================================================
# Release Script for ssproc
# =============================================================================
# This script generates and executes commands to create a clean release branch
# with development files removed, then tags and pushes the release.
# =============================================================================

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# Configuration
MODULE_PATH="github.com/rickchristie/ssproc"
VERSION_FILE="meta.go"
DEV_FILES=(
    ".pgflock"
    ".sandb"
    ".vscode"
    "README.md"
    "scripts"
)

# =============================================================================
# Helper Functions
# =============================================================================

print_header() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BOLD}$1${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

print_step() {
    echo -e "${CYAN}▸${NC} $1"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_command() {
    echo -e "${YELLOW}  \$${NC} $1"
}

confirm() {
    local prompt="$1"
    local response
    echo ""
    read -p "$(echo -e "${BOLD}$prompt${NC} [y/N]: ")" response
    case "$response" in
        [yY][eE][sS]|[yY]) return 0 ;;
        *) return 1 ;;
    esac
}

run_command() {
    local cmd="$1"
    local description="$2"

    echo ""
    print_step "$description"
    print_command "$cmd"

    if confirm "Execute this command?"; then
        eval "$cmd"
        print_success "Done"
        return 0
    else
        print_warning "Skipped"
        return 1
    fi
}

# =============================================================================
# Pre-flight Checks
# =============================================================================

preflight_checks() {
    print_header "Pre-flight Checks"

    # Check if we're in a git repository
    if ! git rev-parse --is-inside-work-tree > /dev/null 2>&1; then
        print_error "Not in a git repository"
        exit 1
    fi
    print_success "Git repository detected"

    # Check for uncommitted changes
    if ! git diff-index --quiet HEAD -- 2>/dev/null; then
        print_error "You have uncommitted changes. Please commit or stash them first."
        echo ""
        git status --short
        exit 1
    fi
    print_success "Working directory is clean"

    # Check if version file exists
    if [[ ! -f "$VERSION_FILE" ]]; then
        print_error "Version file '$VERSION_FILE' not found"
        exit 1
    fi
    print_success "Version file found"

    # Check if on main branch
    local current_branch
    current_branch=$(git branch --show-current)
    if [[ "$current_branch" != "main" && "$current_branch" != "master" ]]; then
        print_warning "You are on branch '$current_branch', not main/master"
        if ! confirm "Continue anyway?"; then
            exit 1
        fi
    else
        print_success "On $current_branch branch"
    fi
}

# =============================================================================
# Version Detection
# =============================================================================

get_version() {
    print_header "Version Detection"

    # Extract version from meta.go
    VERSION=$(grep -oP 'const Version = "\K[^"]+' "$VERSION_FILE")

    if [[ -z "$VERSION" ]]; then
        print_error "Could not extract version from $VERSION_FILE"
        exit 1
    fi

    print_success "Version from $VERSION_FILE: ${BOLD}v$VERSION${NC}"

    # Check if tag already exists
    if git tag -l "v$VERSION" | grep -q "v$VERSION"; then
        print_error "Tag v$VERSION already exists!"
        echo ""
        echo "Recent tags:"
        git tag --list 'v*' --sort=-version:refname | head -5
        exit 1
    fi
    print_success "Tag v$VERSION is available"

    # Get last release tag
    LAST_TAG=$(git tag --list 'v*' --sort=-version:refname | head -1)
    if [[ -n "$LAST_TAG" ]]; then
        print_success "Last release: $LAST_TAG"
    else
        print_warning "No previous releases found"
        LAST_TAG=""
    fi
}

# =============================================================================
# Changelog Generation
# =============================================================================

generate_changelog() {
    print_header "Changelog Preview"

    local log_range
    if [[ -n "$LAST_TAG" ]]; then
        log_range="$LAST_TAG..HEAD"
        echo -e "Commits since ${BOLD}$LAST_TAG${NC}:"
    else
        log_range="HEAD"
        echo -e "All commits (no previous release):"
    fi

    echo ""

    # Get commit messages
    CHANGELOG=""
    while IFS= read -r line; do
        if [[ -n "$line" ]]; then
            echo "  • $line"
            CHANGELOG+="- $line"$'\n'
        fi
    done < <(git log --pretty=format:"%s" $log_range 2>/dev/null | head -50)

    if [[ -z "$CHANGELOG" ]]; then
        print_warning "No commits found for changelog"
        CHANGELOG="- Release v$VERSION"$'\n'
    fi

    echo ""
}

# =============================================================================
# Release Branch Creation
# =============================================================================

create_release_branch() {
    print_header "Create Release Branch"

    RELEASE_BRANCH="release/v$VERSION"

    # Check if branch already exists
    if git show-ref --verify --quiet "refs/heads/$RELEASE_BRANCH"; then
        print_error "Branch $RELEASE_BRANCH already exists locally"
        if confirm "Delete it and recreate?"; then
            run_command "git branch -D $RELEASE_BRANCH" "Delete existing local branch"
        else
            exit 1
        fi
    fi

    run_command "git checkout -b $RELEASE_BRANCH" "Create release branch"
}

# =============================================================================
# Remove Development Files
# =============================================================================

remove_dev_files() {
    print_header "Remove Development Files"

    echo "The following files/directories will be removed from the release:"
    echo ""

    local files_to_remove=()
    for item in "${DEV_FILES[@]}"; do
        if [[ -e "$item" ]]; then
            echo -e "  ${RED}✗${NC} $item"
            files_to_remove+=("$item")
        else
            echo -e "  ${YELLOW}○${NC} $item (not present)"
        fi
    done

    if [[ ${#files_to_remove[@]} -eq 0 ]]; then
        print_warning "No development files to remove"
        return 0
    fi

    echo ""

    if confirm "Remove these files?"; then
        for item in "${files_to_remove[@]}"; do
            rm -rf "$item"
            print_success "Removed $item"
        done

        # Stage removals
        git add -A
        print_success "Staged all changes"
    else
        print_warning "Skipped file removal"
        return 1
    fi
}

# =============================================================================
# Create Release Commit
# =============================================================================

create_release_commit() {
    print_header "Create Release Commit"

    # Build commit message
    COMMIT_MSG="Release v$VERSION

This is a clean release branch with development files removed.

Changes in this release:
$CHANGELOG
Files removed for clean release:
$(printf -- '- %s\n' "${DEV_FILES[@]}")"

    echo "Commit message preview:"
    echo ""
    echo -e "${CYAN}────────────────────────────────────────${NC}"
    echo "$COMMIT_MSG"
    echo -e "${CYAN}────────────────────────────────────────${NC}"

    if confirm "Create this commit?"; then
        git commit -m "$COMMIT_MSG"
        print_success "Created release commit"
    else
        print_warning "Skipped commit creation"
        return 1
    fi
}

# =============================================================================
# Tag Release
# =============================================================================

tag_release() {
    print_header "Tag Release"

    TAG_MSG="v$VERSION"

    run_command "git tag -a v$VERSION -m \"$TAG_MSG\"" "Create annotated tag v$VERSION"
}

# =============================================================================
# Push Release
# =============================================================================

push_release() {
    print_header "Push to Remote"

    run_command "git push -u origin $RELEASE_BRANCH" "Push release branch"
    run_command "git push origin v$VERSION" "Push tag"
}

# =============================================================================
# Go Proxy Update
# =============================================================================

update_go_proxy() {
    print_header "Update Go Module Proxy"

    local proxy_url="https://proxy.golang.org/$MODULE_PATH/@v/v$VERSION.info"

    echo "Request Go proxy to index the new version:"
    print_command "curl -s $proxy_url"

    if confirm "Request proxy update?"; then
        echo ""
        local response
        response=$(curl -s "$proxy_url" 2>&1)

        if echo "$response" | grep -q "\"Version\""; then
            print_success "Go proxy has indexed v$VERSION"
            echo "$response" | head -5
        else
            print_warning "Proxy response (may take a few minutes to index):"
            echo "$response" | head -5
        fi
    fi
}

# =============================================================================
# Return to Main Branch
# =============================================================================

return_to_main() {
    print_header "Cleanup"

    local main_branch
    if git show-ref --verify --quiet refs/heads/main; then
        main_branch="main"
    else
        main_branch="master"
    fi

    run_command "git checkout $main_branch" "Return to $main_branch branch"

    echo ""
    print_success "Release v$VERSION complete!"
    echo ""
    echo "Summary:"
    echo "  • Release branch: $RELEASE_BRANCH"
    echo "  • Tag: v$VERSION"
    echo "  • Module: $MODULE_PATH@v$VERSION"
    echo ""
    echo "Users can now run:"
    print_command "go get $MODULE_PATH@v$VERSION"
}

# =============================================================================
# Main
# =============================================================================

main() {
    echo ""
    echo -e "${BOLD}╔═══════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BOLD}║              ssproc Release Script                        ║${NC}"
    echo -e "${BOLD}╚═══════════════════════════════════════════════════════════╝${NC}"

    preflight_checks
    get_version
    generate_changelog

    echo ""
    echo -e "${BOLD}Ready to create release v$VERSION${NC}"
    if ! confirm "Proceed with release?"; then
        echo "Release cancelled."
        exit 0
    fi

    create_release_branch
    remove_dev_files
    create_release_commit
    tag_release
    push_release
    update_go_proxy
    return_to_main
}

# Run main function
main "$@"
