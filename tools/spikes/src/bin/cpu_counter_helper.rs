use std::{
    env,
    fs::OpenOptions,
    hint::black_box,
    io::{self, Write},
    process::Command,
    thread,
    time::Duration,
};

const USER_ROUNDS: u64 = 120_000_000;
const SYSTEM_ROUNDS: u64 = 500_000;

fn burn_user_cpu() {
    let mut value = 1_u64;
    for _ in 0..USER_ROUNDS {
        value = black_box(value.wrapping_mul(1_664_525).wrapping_add(1_013_904_223));
    }
    black_box(value);
}

fn burn_system_cpu() -> io::Result<()> {
    let mut sink = OpenOptions::new().write(true).open("/dev/null")?;
    for _ in 0..SYSTEM_ROUNDS {
        sink.write_all(b"x")?;
    }
    Ok(())
}

fn burn_both() -> io::Result<()> {
    burn_user_cpu();
    burn_system_cpu()
}

fn main() -> io::Result<()> {
    if env::args_os()
        .nth(1)
        .is_some_and(|value| value == "--child")
    {
        return burn_both();
    }

    thread::sleep(Duration::from_secs(1));
    let mut child = Command::new(env::current_exe()?).arg("--child").spawn()?;
    burn_both()?;
    let status = child.wait()?;
    if !status.success() {
        return Err(io::Error::other("CPU counter child failed"));
    }
    thread::sleep(Duration::from_mins(5));
    Ok(())
}
