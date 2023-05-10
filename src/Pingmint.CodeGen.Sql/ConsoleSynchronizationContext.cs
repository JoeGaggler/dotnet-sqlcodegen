

using System.Collections.Concurrent;
using static System.Console;

namespace Pingmint.CodeGen.Sql.Refactor;

internal record struct SendOrPostCallbackWithState(SendOrPostCallback Callback, Object State);

public class ConsoleSynchronizationContext : SynchronizationContext
{
    private readonly ConcurrentQueue<SendOrPostCallbackWithState> queue = new();
    private Boolean stop = false;
    private int operationCount = 0;

    private static int MainThreadId;
    public static void SetMainThread() { MainThreadId = Environment.CurrentManagedThreadId; }

    [Obsolete("HACK: Remove this method")]
    public static void MainThread() { if (MainThreadId != Environment.CurrentManagedThreadId) throw new Exception("Not main thread"); }

    public void Go(Func<Task> func)
    {
        var previous = SynchronizationContext.Current;
        SynchronizationContext.SetSynchronizationContext(this);
        try
        {
            Thread.CurrentThread.Name = "ConsoleSync";
            operationCount = 1;
            _ = func().ContinueWith((t) => stop = true);
            Interlocked.Decrement(ref operationCount);
            while (true)
            {
                if (!queue.TryDequeue(out var item))
                {
                    if (stop) { break; }
                    Thread.Yield();
                    continue;
                }

                item.Callback(item.State);
            }
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(previous);
        }
    }

    public override void Post(SendOrPostCallback d, object? state)
    {
        queue.Enqueue(new(d, state));
    }

    public override void Send(SendOrPostCallback d, object? state)
    {
        queue.Enqueue(new(d, state));
    }

    public override void OperationStarted()
    {
        base.OperationStarted();
    }

    public override void OperationCompleted()
    {
        base.OperationCompleted();
    }
}
