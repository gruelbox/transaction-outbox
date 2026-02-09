package com.gruelbox.transactionoutbox.virtthreads;

import static org.junit.Assert.assertTrue;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class TestVirtualThreadsH2 extends AbstractVirtualThreadsTest {

  private static final String QSORT = "qsort";

  /**
   * Ensures that the logic we use in {@link AbstractVirtualThreadsTest} to detect thread pinning
   * actually works. In Java 25 we have to turn ourselves inside out to achieve this; even {@code
   * synchronized} doesn't pin a thread anymore. The only thing that seems to work is to make a
   * native call that calls back to Java. When that stops working, this test can probably be
   * removed; there will be very little likelihood of pinning in practice once that last thing is
   * resolved in the JDK.
   */
  @Test
  void forceTriggerPinningViaUpcall() throws Throwable {
    simulateThreadPin();

    // Prevents the check on exit from throwing
    assertTrue(didPin());
  }

  private void simulateThreadPin() throws NoSuchMethodException, IllegalAccessException {
    Linker linker = Linker.nativeLinker();
    SymbolLookup libc =
        name -> {
          try {
            var lookup =
                System.getProperty("os.name").toLowerCase().contains("win")
                    ? SymbolLookup.libraryLookup("msvcrt.dll", Arena.global())
                    : SymbolLookup.libraryLookup("libc.so.6", Arena.global());
            return lookup.find(name);
          } catch (Exception e) {
            return Optional.empty();
          }
        };
    MethodHandle qsort =
        linker.downcallHandle(
            libc.find(QSORT)
                .or(() -> Linker.nativeLinker().defaultLookup().find(QSORT))
                .or(() -> SymbolLookup.loaderLookup().find(QSORT))
                .orElseThrow(() -> new RuntimeException("Could not find qsort")),
            FunctionDescriptor.ofVoid(
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS));
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
