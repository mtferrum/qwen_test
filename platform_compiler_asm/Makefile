# Makefile для Platform Compiler ASM

NASM = nasm
LD = ld
CC = gcc
CFLAGS = -no-pie -fPIC
LDFLAGS = -no-pie

all: platform_compiler_asm

platform_compiler_asm: src/platform_compiler.asm
	$(NASM) -f elf64 -o platform_compiler.o src/platform_compiler.asm
	$(CC) $(CFLAGS) -o platform_compiler_asm platform_compiler.o -lc
	@echo "Сборка завершена успешно"

clean:
	rm -f platform_compiler.o platform_compiler_asm

test: platform_compiler_asm
	python -m pytest tests/test_asm_compiler.py -v

.PHONY: all clean test
