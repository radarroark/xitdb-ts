This is an example program that reads a xitdb file and prints out its most recent value (the last item of the top-level ArrayList). To run it, do:

```
bun install
bun run dump.ts ../tests/fixtures/test.db
```

...and it will print:

```
ArrayList[2]:
    HashMap{7}:
      (none):
        LinkedArrayList[1]:
          [0]:
            "Wash the car"
      (none):
        HashSet{1}: ["a"]
      (none):
        ArrayList[2]:
          [0]:
            HashMap{2}:
              (none):
                26 (uint)
              (none):
                "Alice"
          [1]:
            HashMap{2}:
              (none):
                42 (uint)
              (none):
                "Bob"
      (none):
        "foo"
      "fruits":
        ArrayList[2]:
          [0]:
            "lemon"
          [1]:
            "pear"
      (none):
        CountedHashMap{1}:
          "a":
            2 (uint)
      (none):
        CountedHashSet{1}: ["a"]
```
