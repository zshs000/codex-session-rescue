import sys
from app import main

if "--open" not in sys.argv:
    sys.argv.append("--open")
main()
