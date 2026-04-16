// commit.c — Commit creation and history traversal
//
// Commit object format (stored as text, one field per line):
//
//   tree <64-char-hex-hash>
//   parent <64-char-hex-hash>        ← omitted for the first commit
//   author <name> <unix-timestamp>
//   committer <name> <unix-timestamp>
//
//   <commit message>
//
// Note: there is a blank line between the headers and the message.
//
// PROVIDED functions: commit_parse, commit_serialize, commit_walk, head_read, head_update
// TODO functions:     commit_create

#include "commit.h"
#include "index.h"
#include "tree.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>
#include <dirent.h>

// Forward declarations (implemented in object.c)
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);
int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out);

// ─── PROVIDED ────────────────────────────────────────────────────────────────

// Parse raw commit data into a Commit struct.
int commit_parse(const void *data, size_t len, Commit *commit_out) {
    (void)len;
    const char *p = (const char *)data;
    char hex[HASH_HEX_SIZE + 1];

    // "tree <hex>\n"
    if (sscanf(p, "tree %64s\n", hex) != 1) return -1;
    if (hex_to_hash(hex, &commit_out->tree) != 0) return -1;
    p = strchr(p, '\n') + 1;

    // optional "parent <hex>\n"
    if (strncmp(p, "parent ", 7) == 0) {
        if (sscanf(p, "parent %64s\n", hex) != 1) return -1;
        if (hex_to_hash(hex, &commit_out->parent) != 0) return -1;
        commit_out->has_parent = 1;
        p = strchr(p, '\n') + 1;
    } else {
        commit_out->has_parent = 0;
    }

    // "author <name> <timestamp>\n"
    char author_buf[256];
    uint64_t ts;
    if (sscanf(p, "author %255[^\n]\n", author_buf) != 1) return -1;
    // split off trailing timestamp
    char *last_space = strrchr(author_buf, ' ');
    if (!last_space) return -1;
    ts = (uint64_t)strtoull(last_space + 1, NULL, 10);
    *last_space = '\0';
    strncpy(commit_out->author, author_buf, sizeof(commit_out->author) - 1);
    commit_out->timestamp = ts;
    p = strchr(p, '\n') + 1;  // skip author line
    p = strchr(p, '\n') + 1;  // skip committer line
    p = strchr(p, '\n') + 1;  // skip blank line

    strncpy(commit_out->message, p, sizeof(commit_out->message) - 1);
    return 0;
}

// Serialize a Commit struct to the text format.
// Caller must free(*data_out).
int commit_serialize(const Commit *commit, void **data_out, size_t *len_out) {
    char tree_hex[HASH_HEX_SIZE + 1];
    char parent_hex[HASH_HEX_SIZE + 1];
    hash_to_hex(&commit->tree, tree_hex);

    char buf[8192];
    int n = 0;
    n += snprintf(buf + n, sizeof(buf) - n, "tree %s\n", tree_hex);
    if (commit->has_parent) {
        hash_to_hex(&commit->parent, parent_hex);
        n += snprintf(buf + n, sizeof(buf) - n, "parent %s\n", parent_hex);
    }
    n += snprintf(buf + n, sizeof(buf) - n,
                  "author %s %" PRIu64 "\n"
                  "committer %s %" PRIu64 "\n"
                  "\n"
                  "%s",
                  commit->author, commit->timestamp,
                  commit->author, commit->timestamp,
                  commit->message);

    *data_out = malloc(n + 1);
    if (!*data_out) return -1;
    memcpy(*data_out, buf, n + 1);
    *len_out = (size_t)n;
    return 0;
}

// Walk commit history from HEAD to the root.
int commit_walk(commit_walk_fn callback, void *ctx) {
    ObjectID id;
    if (head_read(&id) != 0) return -1;

    while (1) {
        ObjectType type;
        void *raw;
        size_t raw_len;
        if (object_read(&id, &type, &raw, &raw_len) != 0) return -1;

        Commit c;
        int rc = commit_parse(raw, raw_len, &c);
        free(raw);
        if (rc != 0) return -1;

        callback(&id, &c, ctx);

        if (!c.has_parent) break;
        id = c.parent;
    }
    return 0;
}

// Read the current HEAD commit hash.
int head_read(ObjectID *id_out) {
    FILE *f = fopen(HEAD_FILE, "r");
    if (!f) return -1;
    char line[512];
    if (!fgets(line, sizeof(line), f)) { fclose(f); return -1; }
    fclose(f);
    line[strcspn(line, "\r\n")] = '\0'; // strip newline

    char ref_path[512];
    if (strncmp(line, "ref: ", 5) == 0) {
        snprintf(ref_path, sizeof(ref_path), "%s/%s", PES_DIR, line + 5);
        f = fopen(ref_path, "r");
        if (!f) return -1; // Branch exists but has no commits yet
        if (!fgets(line, sizeof(line), f)) { fclose(f); return -1; }
        fclose(f);
        line[strcspn(line, "\r\n")] = '\0';
    }
    return hex_to_hash(line, id_out);
}

// Update the current branch ref to point to a new commit atomically.
int head_update(const ObjectID *new_commit) {
    FILE *f = fopen(HEAD_FILE, "r");
    if (!f) return -1;
    char line[512];
    if (!fgets(line, sizeof(line), f)) { fclose(f); return -1; }
    fclose(f);
    line[strcspn(line, "\r\n")] = '\0';

    char target_path[512];
    if (strncmp(line, "ref: ", 5) == 0) {
        snprintf(target_path, sizeof(target_path), "%s/%s", PES_DIR, line + 5);
    } else {
        snprintf(target_path, sizeof(target_path), "%s", HEAD_FILE); // Detached HEAD
    }

    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", target_path);
    
    f = fopen(tmp_path, "w");
    if (!f) return -1;
    
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(new_commit, hex);
    fprintf(f, "%s\n", hex);
    
    fflush(f);
    fsync(fileno(f));
    fclose(f);
    
    return rename(tmp_path, target_path);
}

// ─── TODO: Implement these ───────────────────────────────────────────────────

static int write_text_file_atomic(const char *path, const char *text) {
    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", path);

    FILE *f = fopen(tmp_path, "w");
    if (!f) return -1;

    if (fprintf(f, "%s", text) < 0) {
        fclose(f);
        unlink(tmp_path);
        return -1;
    }

    fflush(f);
    if (fsync(fileno(f)) != 0) {
        fclose(f);
        unlink(tmp_path);
        return -1;
    }

    fclose(f);
    if (rename(tmp_path, path) != 0) {
        unlink(tmp_path);
        return -1;
    }

    return 0;
}

static int ensure_pes_directories(void) {
    if (mkdir(PES_DIR, 0755) != 0 && errno != EEXIST) return -1;
    if (mkdir(OBJECTS_DIR, 0755) != 0 && errno != EEXIST) return -1;
    if (mkdir(PES_DIR "/refs", 0755) != 0 && errno != EEXIST) return -1;
    if (mkdir(REFS_DIR, 0755) != 0 && errno != EEXIST) return -1;
    return 0;
}

static void print_commit_callback(const ObjectID *id, const Commit *commit, void *ctx) {
    (void)ctx;
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    printf("commit %s\n", hex);
    printf("    %s\n\n", commit->message);
}

int commit_create(const char *message, ObjectID *commit_id_out) {
    if (ensure_pes_directories() != 0) return -1;

    ObjectID tree_id;
    if (tree_from_index(&tree_id) != 0) return -1;

    Commit commit;
    memset(&commit, 0, sizeof(commit));
    commit.tree = tree_id;
    commit.has_parent = 0;
    if (head_read(&commit.parent) == 0) {
        commit.has_parent = 1;
    }

    strncpy(commit.author, pes_author(), sizeof(commit.author) - 1);
    commit.author[sizeof(commit.author) - 1] = '';
    commit.timestamp = (uint64_t)time(NULL);
    strncpy(commit.message, message, sizeof(commit.message) - 1);
    commit.message[sizeof(commit.message) - 1] = '';

    void *data;
    size_t len;
    if (commit_serialize(&commit, &data, &len) != 0) return -1;

    if (object_write(OBJ_COMMIT, data, len, commit_id_out) != 0) {
        free(data);
        return -1;
    }
    free(data);

    if (head_update(commit_id_out) != 0) return -1;
    return 0;
}

void cmd_init(void) {
    if (ensure_pes_directories() != 0) {
        fprintf(stderr, "error: failed to create .pes repository\n");
        return;
    }

    if (write_text_file_atomic(HEAD_FILE, "ref: refs/heads/main\n") != 0) {
        fprintf(stderr, "error: failed to initialize HEAD\n");
    }
}

void cmd_add(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: pes add <file>...\n");
        return;
    }

    if (ensure_pes_directories() != 0) {
        fprintf(stderr, "error: repository not initialized\n");
        return;
    }

    Index index;
    if (index_load(&index) != 0) {
        fprintf(stderr, "error: could not load index\n");
        return;
    }

    for (int i = 2; i < argc; i++) {
        if (index_add(&index, argv[i]) != 0) {
            fprintf(stderr, "error: failed to add '%s'\n", argv[i]);
            return;
        }
    }
}

void cmd_status(void) {
    Index index;
    if (index_load(&index) != 0) {
        fprintf(stderr, "error: could not load index\n");
        return;
    }
    index_status(&index);
}

void cmd_commit(int argc, char *argv[]) {
    if (argc != 4 || strcmp(argv[2], "-m") != 0) {
        fprintf(stderr, "Usage: pes commit -m <message>\n");
        return;
    }

    ObjectID commit_id;
    if (commit_create(argv[3], &commit_id) != 0) {
        fprintf(stderr, "error: commit failed\n");
        return;
    }
}

void cmd_log(void) {
    if (commit_walk(print_commit_callback, NULL) != 0) {
        fprintf(stderr, "error: no commits yet\n");
    }
}

void branch_list(void) {
    DIR *dir = opendir(REFS_DIR);
    if (!dir) {
        fprintf(stderr, "error: repository not initialized\n");
        return;
    }

    struct dirent *ent;
    while ((ent = readdir(dir)) != NULL) {
        if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
        printf("%s\n", ent->d_name);
    }
    closedir(dir);
}

int branch_create(const char *name) {
    if (!name || name[0] == '' || strchr(name, "/") != NULL) return -1;
    if (ensure_pes_directories() != 0) return -1;

    ObjectID id;
    if (head_read(&id) != 0) return -1;

    char path[512];
    snprintf(path, sizeof(path), "%s/%s", REFS_DIR, name);

    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(&id, hex);
    return write_text_file_atomic(path, hex);
}

int branch_delete(const char *name) {
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", REFS_DIR, name);
    return unlink(path) == 0 ? 0 : -1;
}

int checkout(const char *target) {
    if (!target || target[0] == '') return -1;

    char branch_path[512];
    snprintf(branch_path, sizeof(branch_path), "%s/%s", REFS_DIR, target);

    if (access(branch_path, F_OK) == 0) {
        char head_text[512];
        snprintf(head_text, sizeof(head_text), "ref: refs/heads/%s\n", target);
        return write_text_file_atomic(HEAD_FILE, head_text);
    }

    ObjectID id;
    if (hex_to_hash(target, &id) != 0) return -1;

    ObjectType type;
    void *raw;
    size_t len;
    if (object_read(&id, &type, &raw, &len) != 0) return -1;
    free(raw);

    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(&id, hex);
    char head_text[512];
    snprintf(head_text, sizeof(head_text), "%s\n", hex);
    return write_text_file_atomic(HEAD_FILE, head_text);
}
