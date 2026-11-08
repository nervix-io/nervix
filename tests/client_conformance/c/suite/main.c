/* The C11 application suite. Every test tears down its owned objects before its result line.
 * The fixed cleanup stack bounds a test's live allocations and preserves teardown on failure. */
#define _POSIX_C_SOURCE 200809L
#include <setjmp.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct suite_owned { void *pointer; void (*release)(void *); } suite_owned;
static suite_owned suite_objects[512];
static size_t suite_object_count;
static jmp_buf suite_failure;

static void suite_require(bool passed, const char *expression) {
    if (!passed) {
        fprintf(stderr, "assertion failed: %s\n", expression);
        longjmp(suite_failure, 1);
    }
}

static void suite_hold(void *pointer, void (*release)(void *)) {
    suite_require(pointer != NULL, "an owned allocation is present");
    if (suite_object_count == sizeof suite_objects / sizeof suite_objects[0]) {
        release(pointer);
        suite_require(false, "a test fits the 512-allocation bound");
    }
    suite_objects[suite_object_count].pointer = pointer;
    suite_objects[suite_object_count].release = release;
    ++suite_object_count;
}

static void *suite_forget(void *pointer) {
    for (size_t i = suite_object_count; i > 0; --i) {
        if (suite_objects[i - 1].pointer == pointer) {
            memmove(&suite_objects[i - 1], &suite_objects[i],
                    (suite_object_count - i) * sizeof suite_objects[0]);
            --suite_object_count;
            return pointer;
        }
    }
    suite_require(false, "a transferred object has one owned reference");
    return NULL;
}

static void suite_drop(void *pointer) {
    for (size_t i = suite_object_count; i > 0; --i) {
        if (suite_objects[i - 1].pointer == pointer) {
            void (*release)(void *) = suite_objects[i - 1].release;
            suite_forget(pointer);
            release(pointer);
            return;
        }
    }
    suite_require(false, "an explicitly released object is owned");
}

static size_t suite_mark(void) { return suite_object_count; }
static void suite_cleanup_to(size_t mark) {
    while (suite_object_count > mark) {
        suite_owned owned = suite_objects[--suite_object_count];
        owned.release(owned.pointer);
    }
}

#include "contract.h"

static const struct suite_test suite_tests[] = { SUITE_CONTRACT_TESTS };

int main(int argc, char **argv) {
    if (argc != 2 || (strcmp(argv[1], "boundary") && strcmp(argv[1], "cluster"))) {
        fprintf(stderr, "usage: c-suite boundary|cluster\n");
        return 2;
    }
    size_t ran = 0;
    int failed = 0;
    for (size_t i = 0; i < sizeof suite_tests / sizeof suite_tests[0]; ++i) {
        if (strcmp(suite_tests[i].tier, argv[1])) continue;
        ++ran;
        int result = setjmp(suite_failure);
        if (result == 0) suite_tests[i].run();
        suite_cleanup_to(0);
        if (!suite_clear_refusals()) {
            fprintf(stderr, "scenario did not confirm refusal cleanup\n");
            result = 1;
        }
        printf("NERVIX-SUITE %s %s\n", result ? "failed" : "passed", suite_tests[i].name);
        fflush(stdout);
        if (result) failed = 1;
    }
    return ran ? failed : 2;
}
