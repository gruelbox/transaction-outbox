package com.gruelbox.transactionoutbox.virtthreads;

import static org.junit.Assert.assertTrue;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class TestVirtualThreadsH2 extends AbstractVirtualThreadsTest {

  @Test
  void forceTriggerPinningViaUpcall() throws Throwable {
    simulateThreadPin();
    assertTrue(didPin());
  }

  private void simulateThreadPin() throws NoSuchMethodException, IllegalAccessException {
    Linker linker = Linker.nativeLinker();

    // Find qsort (available in libc/msvcrt)
    SymbolLookup libc =
        System.getProperty("os.name").toLowerCase().contains("win")
            ? SymbolLookup.libraryLookup("msvcrt.dll", Arena.global())
            : SymbolLookup.loaderLookup();

    MethodHandle qsort =
        linker.downcallHandle(
            libc.find("qsort").orElseThrow(),
            FunctionDescriptor.ofVoid(
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS));

    // Create an upcall to our Java 'block' method
    MethodHandle upcallTarget =
        MethodHandles.lookup()
            .findStatic(
                getClass(),
                "javaBlockCallback",
                MethodType.methodType(int.class, MemorySegment.class, MemorySegment.class));

    MemorySegment upcallStub =
        linker.upcallStub(
            upcallTarget,
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS),
            Arena.global());

    // Run in a virtual thread
    Thread.ofVirtual()
        .name("Pin-Me-Thread")
        .start(
            () -> {
              try {
                System.out.println("Virtual thread entering native qsort...");
                MemorySegment data = Arena.ofConfined().allocate(16); // 2 long elements
                qsort.invoke(data, 2L, 8L, upcallStub);
                System.out.println("Virtual thread exited native qsort.");
              } catch (Throwable e) {
                throw new RuntimeException(e);
              }
            });
  }

  // This is the Upcall: Java -> Native (qsort) -> Java (here)
  public static int javaBlockCallback(MemorySegment a, MemorySegment b) {
    try {
      // This 'park' operation happens while a native frame (qsort)
      // is on the stack. Pinning is guaranteed.
      Thread.sleep(200);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return 0;
  }
}
