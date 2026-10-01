import sys, tty, termios, time
def test():
    fd = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    try:
        tty.setcbreak(fd)
        sys.stdout.write("> ")
        sys.stdout.flush()
        buf = ""
        while True:
            ch = sys.stdin.read(1)
            if ch == "\n":
                # sys.stdout.write("\n") # simulate what's currently there
                # sys.stdout.flush()
                # we will skip it
                sys.stdout.write("\r\033[2K> ")
                sys.stdout.flush()
                # simulate server reply
                time.sleep(0.1)
                sys.stdout.write(f"\r\033[2K[Server]: {buf}\n> ")
                sys.stdout.flush()
                buf = ""
            elif ch == "q":
                break
            else:
                buf += ch
                sys.stdout.write(ch)
                sys.stdout.flush()
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)
test()
