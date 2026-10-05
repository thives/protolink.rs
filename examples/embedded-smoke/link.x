/* Generic Cortex-M0 image layout, not a particular board's memory map. */
MEMORY
{
  FLASH (rx)  : ORIGIN = 0x00000000, LENGTH = 256K
  RAM   (rwx) : ORIGIN = 0x20000000, LENGTH = 64K
}

ENTRY(Reset)
__stack_top = ORIGIN(RAM) + LENGTH(RAM);
__stack_bottom = __stack_top - 16K;

SECTIONS
{
  .vector_table ORIGIN(FLASH) : ALIGN(256)
  {
    KEEP(*(.vector_table));
  } > FLASH

  .text : ALIGN(4)
  {
    *(.text .text.*);
    *(.rodata .rodata.*);
    . = ALIGN(4);
  } > FLASH

  .ARM.exidx : ALIGN(4)
  {
    *(.ARM.exidx .ARM.exidx.*);
  } > FLASH

  .ARM.extab : ALIGN(4)
  {
    *(.ARM.extab .ARM.extab.*);
  } > FLASH

  .data : ALIGN(4)
  {
    __sdata = .;
    *(.data .data.*);
    . = ALIGN(4);
    __edata = .;
  } > RAM AT> FLASH
  __sidata = LOADADDR(.data);

  .bss (NOLOAD) : ALIGN(4)
  {
    __sbss = .;
    *(.bss .bss.*);
    *(COMMON);
    . = ALIGN(4);
    __ebss = .;
  } > RAM

  .stack __stack_bottom (NOLOAD) : ALIGN(8)
  {
    . += 16K;
  } > RAM

  /DISCARD/ :
  {
    *(.eh_frame .eh_frame.*);
  }
}

ASSERT(SIZEOF(.vector_table) == 192, "Cortex-M0 vector table must have 48 entries");
ASSERT(__ebss <= __stack_bottom, "RAM globals/heap overlap the reserved stack");
ASSERT((__stack_top & 7) == 0, "initial stack must be 8-byte aligned");
