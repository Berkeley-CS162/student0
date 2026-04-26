#include "persister.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void persister_init(persister_t *p, const char *path) {
    snprintf(p->path, sizeof(p->path), "%s", path);
}

void persister_save(persister_t *p, const char *data) {
    char tmp[520];
    snprintf(tmp, sizeof(tmp), "%s.tmp", p->path);

    FILE *f = fopen(tmp, "w");
    if (!f) return;
    size_t len = strlen(data);
    fwrite(data, 1, len, f);
    fclose(f);

    rename(tmp, p->path);
}

char *persister_read(persister_t *p) {
    FILE *f = fopen(p->path, "r");
    if (!f) return NULL;

    fseek(f, 0, SEEK_END);
    long len = ftell(f);
    fseek(f, 0, SEEK_SET);

    char *buf = malloc(len + 1);
    if (len > 0)
        fread(buf, 1, len, f);
    buf[len] = '\0';
    fclose(f);
    return buf;
}
