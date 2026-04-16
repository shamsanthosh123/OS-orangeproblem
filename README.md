# PES VCS (Version Control System)

## 📌 Overview
This project is a simplified version of a Version Control System (like Git), implemented as part of the PES University lab. It supports basic version control operations such as initializing a repository, adding files, committing changes, and viewing commit history.

---

## ⚙️ Features
- Initialize repository (`pes init`)
- Add files to staging area (`pes add`)
- Create commits (`pes commit`)
- View commit history (`pes log`)
- Object storage system
- Tree structure for file snapshots

---

## 🛠️ Technologies Used
- C Programming Language
- Linux (Ubuntu)
- GCC Compiler
- Makefile

---

## 📂 Project Structure


.
├── commit.c
├── tree.c
├── index.c
├── pes.c
├── Makefile
├── report.pdf
└── .pes/ (internal storage)


---

## 🚀 How to Run

### Step 1: Compile
```bash
make
Step 2: Initialize Repository
./pes init
Step 3: Add Files
echo "Hello" > file.txt
./pes add file.txt
Step 4: Commit
./pes commit -m "Initial commit"
Step 5: View Log
./pes log
📸 Screenshots
Screenshot 4A – Commit Log

Shows commit history with hash, author, timestamp, and message.

Screenshot 4B – Object Store

Displays internal storage structure:

find .pes -type f | sort
Screenshot 4C – Reference Chain
cat .pes/refs/heads/main
cat .pes/HEAD
🧠 Concepts Used
Version Control Systems
File Snapshot using Trees
Hash-based Object Storage
Linked Commit History (Parent commits)
Staging Area (Index)
📝 Commit Workflow
Files added to index
Tree created from index
Commit created with:
Tree hash
Parent commit
Author
Timestamp
Message
Stored as object
HEAD updated
👨‍💻 Author

SHAM SANTHOSH S

Submission Checklist
 Code implemented
 Repository created (public)
 Report uploaded (report.pdf)
 Screenshots included
 Minimum 5 commits
 Conclusion

This project demonstrates the internal working of a version control system and provides hands-on experience with core concepts like commits, trees, and object storage.


---

## ✅ Step 1:
Create file:

```bash
touch README.md
