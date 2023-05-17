

using System.Collections.Concurrent;
using static System.Console;

namespace Pingmint.CodeGen.Sql.Refactor;

internal record struct SendOrPostCallbackWithState(SendOrPostCallback Callback, Object? State);

public class ConsoleSynchronizationContext : SynchronizationContext
{
    private readonly ConcurrentQueue<SendOrPostCallbackWithState> queue = new();
    private Boolean stop = false;
    private int operationCount = 0;

    public void Go(Func<Task> func)
    {
        var previous = SynchronizationContext.Current;
        SynchronizationContext.SetSynchronizationContext(this);
        try
        {
            Thread.CurrentThread.Name = "ConsoleSync";
            operationCount = 1;
            Exception? exception = null;
            _ = func().ContinueWith((t) =>
            {
                exception = t.Exception;
                stop = true;
            });

            while (true)
            {
                if (!queue.TryDequeue(out var item))
                {
                    if (stop)
                    {
                        //WriteLine("----- Stopping Context -----");
                        break;
                    }
                    Thread.Yield();
                    continue;
                }
                //WriteLine("----- Continuation -----");
                item.Callback(item.State);
            }

            if (exception is not null) { throw exception; }
        }
        finally
        {
            //WriteLine("----- Revert Context -----");
            SynchronizationContext.SetSynchronizationContext(previous);
        }
    }

    public override void Post(SendOrPostCallback d, object? state)
    {
        //WriteLine("----- Post -----");
        queue.Enqueue(new(d, state));
    }

    public override void Send(SendOrPostCallback d, object? state)
    {
        //WriteLine("----- Send -----");
        queue.Enqueue(new(d, state));
    }

    public override void OperationStarted()
    {
        //WriteLine("----- Op: Start -----");
        base.OperationStarted();
    }

    public override void OperationCompleted()
    {
        //WriteLine("----- Op: Done -----");
        base.OperationCompleted();
    }
}
