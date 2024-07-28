import time

words = [" _.", "  ._", ". _.", "_..", "__.", "=.", "._."]
for letter in "HELLO":
    if letter == 'L':
        word = words[3]
    elif letter == 'E':
        word = words[4]
    elif letter == 'O':
        word = words[5]
    else:
        word = words[1 + ord(letter) - ord('A')]
    
    for i, c in enumerate(word):
        print(c + ('' if i < len(word)-2 else '\b\b', end="") )
        time.sleep(0.05)
