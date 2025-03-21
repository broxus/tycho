use anyhow::Result;
use clap::CommandFactory;

/// Work with shell environment.
#[derive(clap::Parser)]
pub struct Cmd {
    #[clap(subcommand)]
    cmd: SubCmd,
}

#[derive(clap::Subcommand)]
pub enum SubCmd {
    /// Print a CLI help for all subcommands as Markdown.
    MarkdownHelp,
}

impl Cmd {
    pub fn run(&self) -> Result<()> {
        match self.cmd {
            SubCmd::MarkdownHelp => {
                print_help();
            }
        }

        Ok(())
    }
}

fn print_help() {
    let command = crate::App::command();

    let markdown = clap_markdown::help_markdown_command(&command);
    println!("{}", markdown);
}
