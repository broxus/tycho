## Standard Process

Below is the normal procedure that you're likely to use for most minor changes
and PRs:

1. Ensure that you're making your changes on top of master:
   `git checkout master`.
2. Get the latest changes from the Rust
   repo: `git pull upstream master --ff-only`.
   (see [No-Merge Policy][no-merge-policy] for more info about this).
3. Make a new branch for your change: `git checkout -b issue-12345-fix`.
4. Make some changes to the repo and test them.
5. Stage your changes via `git add src/changed/file.rs src/another/change.rs`
   and then commit them with `git commit`. Of course, making intermediate
   commits
   may be a good idea as well. Avoid `git add .`, as it makes it too easy to
   unintentionally commit changes that should not be committed, such as
   submodule
   updates. You can use `git status` to check if there are any files you forgot
   to stage.
6. Push your changes to your
   fork: `git push --set-upstream origin issue-12345-fix`
   (After adding commits, you can use `git push` and after rebasing or
   pulling-and-rebasing, you can use `git push --force-with-lease`).
7. [Open a PR][ghpullrequest] from your fork to `broxus/tycho`'s master
   branch.

[ghpullrequest]: https://guides.github.com/activities/forking/#making-a-pull-request

If your reviewer requests changes, the procedure for those changes looks much
the same, with some steps skipped:

1. Ensure that you're making changes to the most recent version of your code:
   `git checkout issue-12345-fix`.
2. Make, stage, and commit your additional changes just like before.
3. Push those changes to your fork: `git push`.

[no-merge-policy]: #keeping-things-up-to-date

## Troubleshooting git issues

You don't need to clone `broxus/tycho` from scratch if it's out of date!
Even if you think you've messed it up beyond repair, there are ways to fix
the git state that don't require downloading the whole repository again.
Here are some common issues you might run into:

### I made a merge commit by accident.

Git has two ways to update your branch with the newest changes: merging and
rebasing.
Rust [uses rebasing][no-merge-policy]. If you make a merge commit, it's not too
hard to fix:
`git rebase -i upstream/master`.

See [Rebasing](#rebasing) for more about rebasing.

### I see "error: cannot rebase" when I try to rebase

These are two common errors to see when rebasing:

```
error: cannot rebase: Your index contains uncommitted changes.
error: Please commit or stash them.
```

```
error: cannot rebase: You have unstaged changes.
error: Please commit or stash them.
```

(
See <https://git-scm.com/book/en/v2/Getting-Started-What-is-Git%3F#_the_three_states>
for the difference between the two.)

This means you have made changes since the last time you made a commit. To be
able to rebase, either
commit your changes, or make a temporary commit called a "stash" to have them
still not be committed
when you finish rebasing. You may want to configure git to make this "stash"
automatically, which
will prevent the "cannot rebase" error in nearly all cases:

```
git config --global rebase.autostash true
```

See <https://git-scm.com/book/en/v2/Git-Tools-Stashing-and-Cleaning> for more
info about stashing.

### failed to push some refs

`git push` will not work properly and say something like this:

```
 ! [rejected]        issue-xxxxx -> issue-xxxxx (non-fast-forward)
error: failed to push some refs to 'https://github.com/username/rust.git'
hint: Updates were rejected because the tip of your current branch is behind
hint: its remote counterpart. Integrate the remote changes (e.g.
hint: 'git pull ...') before pushing again.
hint: See the 'Note about fast-forwards' in 'git push --help' for details.
```

The advice this gives is incorrect! Because of Rust's
["no-merge" policy](#no-merge-policy) the merge commit created by `git pull`
will not be allowed in the final PR, in addition to defeating the point of the
rebase! Use `git push --force-with-lease` instead.

### Git is trying to rebase commits I didn't write?

If you see many commits in your rebase list, or merge commits, or commits by
other people that you
didn't write, it likely means you're trying to rebase over the wrong branch. For
example, you may
have a `broxus/tycho` remote `upstream`, but ran `git rebase origin/master`
instead of `git rebase upstream/master`. The fix is to abort the rebase and use the correct branch
instead:

```
git rebase --abort
git rebase -i upstream/master
```

## Rebasing and Conflicts

When you edit your code locally, you are making changes to the version of
broxus/tycho that existed when you created your feature branch. As such, when
you submit your PR it is possible that some of the changes that have been made
to broxus/tycho since then are in conflict with the changes you've made.
When this happens, you need to resolve the conflicts before your changes can be
merged. To do that, you need to rebase your work on top of broxus/tycho.

### Rebasing

To rebase your feature branch on top of the newest version of the master branch
of broxus/tycho, checkout your branch, and then run this command:

```
git pull --rebase https://github.com/broxus/tycho.git master
```

> If you are met with the following error:
>
> ```
> error: cannot pull with rebase: Your index contains uncommitted changes.
> error: please commit or stash them.
> ```
>
> it means that you have some uncommitted work in your working tree. In that
> case, run `git stash` before rebasing, and then `git stash pop` after you
> have rebased and fixed all conflicts.

When you rebase a branch on master, all the changes on your branch are
reapplied to the most recent version of master. In other words, Git tries to
pretend that the changes you made to the old version of master were instead
made to the new version of master. During this process, you should expect to
encounter at least one "rebase conflict." This happens when Git's attempt to
reapply the changes fails because your changes conflicted with other changes
that have been made. You can tell that this happened because you'll see
lines in the output that look like

```
CONFLICT (content): Merge conflict in file.rs
```

When you open these files, you'll see sections of the form

```
<<<<<<< HEAD
Original code
=======
Your code
>>>>>>> 8fbf656... Commit fixes 12345
```

This represents the lines in the file that Git could not figure out how to
rebase. The section between `<<<<<<< HEAD` and `=======` has the code from
master, while the other side has your version of the code. You'll need to
decide how to deal with the conflict. You may want to keep your changes,
keep the changes on master, or combine the two.

Generally, resolving the conflict consists of two steps: First, fix the
particular conflict. Edit the file to make the changes you want and remove the
`<<<<<<<`, `=======` and `>>>>>>>` lines in the process. Second, check the
surrounding code. If there was a conflict, its likely there are some logical
errors lying around too! It's a good idea to run `x check` here to make sure
there are no glaring errors.

Once you're all done fixing the conflicts, you need to stage the files that had
conflicts in them via `git add`. Afterwards, run `git rebase --continue` to let
Git know that you've resolved the conflicts and it should finish the rebase.

Once the rebase has succeeded, you'll want to update the associated branch on
your fork with `git push --force-with-lease`.

### Keeping things up to date

The above section on [Rebasing](#rebasing) is a specific
guide on rebasing work and dealing with merge conflicts.
Here is some general advice about how to keep your local repo
up-to-date with upstream changes:

Using `git pull upstream master` while on your local master branch regularly
will keep it up-to-date. You will also want to rebase your feature branches
up-to-date as well. After pulling, you can checkout the feature branches
and rebase them:

```
git checkout master
git pull upstream master --ff-only # to make certain there are no merge commits
git rebase master feature_branch
git push --force-with-lease # (set origin to be the same as local)
```

To avoid merges as per the [No-Merge Policy](#no-merge-policy), you may want to
use
`git config pull.ff only` (this will apply the config only to the local repo)
to ensure that Git doesn't create merge commits when `git pull`ing, without
needing to pass `--ff-only` or `--rebase` every time.

You can also `git push --force-with-lease` from master to keep your fork's
master in sync with
upstream.

## Advanced Rebasing

### Squash your commits

If your branch contains multiple consecutive rewrites of the same code, or if
the rebase conflicts are extremely severe, you can use
`git rebase --interactive master` to gain more control over the process. This
allows you to choose to skip commits, edit the commits that you do not skip,
change the order in which they are applied, or "squash" them into each other.

Alternatively, you can sacrifice the commit history like this:

```
# squash all the changes into one commit so you only have to worry about conflicts once
git rebase -i $(git merge-base master HEAD)  # and squash all changes along the way
git rebase master
# fix all merge conflicts
git rebase --continue
```

"Squashing" commits into each other causes them to be merged into a single
commit. Both the upside and downside of this is that it simplifies the history.
On the one hand, you lose track of the steps in which changes were made, but
the history becomes easier to work with.

You also may want to squash just the last few commits together, possibly
because they only represent "fixups" and not real changes. For example,
`git rebase --interactive HEAD~2` will allow you to edit the two commits only.

### `git range-diff`

After completing a rebase, and before pushing up your changes, you may want to
review the changes between your old branch and your new one. You can do that
with `git range-diff master @{upstream} HEAD`.

The first argument to `range-diff`, `master` in this case, is the base revision
that you're comparing your old and new branch against. The second argument is
the old version of your branch; in this case, `@upstream` means the version that
you've pushed to GitHub, which is the same as what people will see in your pull
request. Finally, the third argument to `range-diff` is the _new_ version of
your branch; in this case, it is `HEAD`, which is the commit that is currently
checked-out in your local repo.

Note that you can also use the equivalent, abbreviated form `git range-diff master @{u} HEAD`.

Unlike in regular Git diffs, you'll see a `-` or `+` next to another `-` or `+`
in the range-diff output. The marker on the left indicates a change between the
old branch and the new branch, and the marker on the right indicates a change
you've committed. So, you can think of a range-diff as a "diff of diffs" since
it shows you the differences between your old diff and your new diff.

Here's an example of `git range-diff` output (taken from [Git's
docs][range-diff-example-docs]):

```
-:  ------- > 1:  0ddba11 Prepare for the inevitable!
1:  c0debee = 2:  cab005e Add a helpful message at the start
2:  f00dbal ! 3:  decafe1 Describe a bug
    @@ -1,3 +1,3 @@
     Author: A U Thor <author@example.com>

    -TODO: Describe a bug
    +Describe a bug
    @@ -324,5 +324,6
      This is expected.

    -+What is unexpected is that it will also crash.
    ++Unexpectedly, it also crashes. This is a bug, and the jury is
    ++still out there how to fix it best. See ticket #314 for details.

      Contact
3:  bedead < -:  ------- TO-UNDO
```

(Note that `git range-diff` output in your terminal will probably be easier to
read than in this example because it will have colors.)

Another feature of `git range-diff` is that, unlike `git diff`, it will also
diff commit messages. This feature can be useful when amending several commit
messages so you can make sure you changed the right parts.

`git range-diff` is a very useful command, but note that it can take some time
to get used to its output format. You may also find Git's documentation on the
command useful, especially their ["Examples" section][range-diff-example-docs].

[range-diff-example-docs]: https://git-scm.com/docs/git-range-diff#_examples

## No-Merge Policy

The broxus/tycho repo uses what is known as a "rebase workflow." This means
that merge commits in PRs are not accepted. As a result, if you are running
`git merge` locally, chances are good that you should be rebasing instead. Of
course, this is not always true; if your merge will just be a fast-forward,
like the merges that `git pull` usually performs, then no merge commit is
created and you have nothing to worry about. Running `git config merge.ff only`
(this will apply the config to the local repo)
once will ensure that all the merges you perform are of this type, so that you
cannot make a mistake.

There are a number of reasons for this decision and like all others, it is a
tradeoff. The main advantage is the generally linear commit history. This
greatly simplifies bisecting and makes the history and commit log much easier
to follow and understand.

## Tips for reviewing

**NOTE**: This section is for _reviewing_ PRs, not authoring them.

### Fetching PRs

To checkout PRs locally, you can
use `git fetch upstream pull/NNNNN/head && git checkout FETCH_HEAD`.

You can also use github's cli tool. Github shows a button on PRs where you can
copy-paste the
command to check it out locally. See <https://cli.github.com/> for more info.

### Moving large sections of code

Git and Github's default diff view for large moves _within_ a file is quite
poor; it will show each
line as deleted and each line as added, forcing you to compare each line
yourself. Git has an option
to show moved lines in a different color:

```
git log -p --color-moved=dimmed-zebra --color-moved-ws=allow-indentation-change
```

See [the docs for `--color-moved`](https://git-scm.com/docs/git-diff#Documentation/git-diff.txt---color-movedltmodegt)
for more info.

### range-diff

See [the relevant section for PR authors](#git-range-diff). This can be useful
for comparing code
that was force-pushed to make sure there are no unexpected changes.

### Ignoring changes to specific files

Many large files in the repo are autogenerated. To view a diff that ignores
changes to those files,
you can use the following syntax (e.g. Cargo.lock):

```
git log -p ':!Cargo.lock'
```

Arbitrary patterns are supported (e.g. `:!compiler/*`). Patterns use the same
syntax as
`.gitignore`, with `:` prepended to indicate a pattern.
