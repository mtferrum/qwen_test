# Platform Compiler ASM

Assembly реализация компилятора платформенного кода.

## Требования

- NASM (Netwide Assembler)
- GCC (для линковки)
- Linux x86_64

## Сборка

```bash
make
```

## Использование

```bash
./platform_compiler_asm pipeline.dsl platform.yaml output/
```

## Архитектура

Код написан для x86_64 Linux (System V AMD64 ABI).
Использует внешние функции из libc для:
- Файлового ввода/вывода (fopen, fread, fwrite)
- Работы со строками (strcmp, strstr, strcpy)
- Управления памятью (malloc, free)

## Тестирование

```bash
make test
```
