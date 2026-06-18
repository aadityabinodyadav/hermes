import re
import subprocess

file_path = r"c:\Users\Aaditya\Desktop\projects\distributed\hermes\pkg\raft\raft.go"
with open(file_path, "r", encoding="utf-8") as f:
    text = f.read()

# Fix the duplicate case MsgVote error
text = re.sub(r'case MsgVote:\s*r\.handleVoteRequest\(m\)\s*case MsgPropose:', r'case MsgPropose:', text)

def remove_comments(text):
    pattern = r'(".*?"|\'.*?\'|`.*?`)|(/\*.*?\*/|//[^\n]*)'
    def replacer(match):
        if match.group(2) is not None:
            return ""
        return match.group(1)
    return re.sub(pattern, replacer, text, flags=re.MULTILINE | re.DOTALL)

text = remove_comments(text)

# Write back
with open(file_path, "w", encoding="utf-8") as f:
    f.write(text)
