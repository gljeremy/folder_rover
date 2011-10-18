using System;
using System.Collections.Generic;
using System.Threading;
using System.IO;
using System.Text;

namespace FolderRover
{
  /// <summary>
  /// Command line utility designed to quickly traverse the extent of a directory tree and inventory every file/directory/permission problem it has.
  /// For each subdirectory - a new thread is spawned, files and folders are recorded, and new threads are created for each nested directory.
  /// </summary>
  class FileInventory
  {
    #region Variables/Consts
    private static FileStream _exceptionFs = null;
    private static FileStream _fileStream = null;
    private static FileStream _directoryStream = null;
    private static TextWriter _fileFileWriter = null;
    private static TextWriter _dirFileWriter = null;

    private static int _fileCount = 0;
    private static int _dirCount = 0;
    private static int _exceptionCount = 0;

    private const int MaxWaitHandles = 60;
    #endregion

    #region Main(...)
    /// <summary>
    /// Yeah, you know.
    /// </summary>
    /// <param name="args">Command line arguments. Root path and output path are expected.</param>
    public static void Main(string[] args)
    {
      try
      {
        if (args.Length != 2)
        {
          Console.WriteLine("\nUsage: FolderRover <root dir path> <output dir path>\n");
          Console.WriteLine("FolderRover will recursively traverse the contents of the directory specified by <root dir path>.");
          Console.WriteLine("A count of the number of files and directories encountered will be displayed in real time and an inventory ");
          Console.WriteLine("of every file and directory will be stored in the specified output directory.");
          Console.WriteLine("Three files will be written to the output directory:");
          Console.WriteLine("1. Dirs.txt - contains every directory encountered.");
          Console.WriteLine("2. Files.txt - contains the full path of every file encountered.");
          Console.WriteLine("3. FileInventory_Exceptions.txt - Any problems reading files/directories will be logged here.\n");
          return;
        }

        _exceptionFs = new FileStream(Path.Combine(args[1], "FolderRover_Exceptions.txt"), FileMode.Create, FileAccess.Write);
        _fileStream = new FileStream(Path.Combine(args[1], "FileInventory.txt"), FileMode.Create, FileAccess.Write);
        _directoryStream = new FileStream(Path.Combine(args[1], "DirInventory.txt"), FileMode.Create, FileAccess.Write);

        _fileFileWriter = TextWriter.Synchronized(new StreamWriter(_fileStream));
        _dirFileWriter = TextWriter.Synchronized(new StreamWriter(_directoryStream));

        RoverTheFiles(args[0]);

        Console.WriteLine("Processing complete.");
      }
      catch (Exception ex)
      {
        Console.WriteLine(ex.ToString());
      }
    }
    #endregion

    #region Private Methods
    /// <summary>
    /// Inventory files all files under the root directory
    /// </summary>
    /// <param name="dirRoot">Root path to start roving from</param>
    private static void RoverTheFiles(string dirRoot)
    {
      try
      {
        ThreadPool.QueueUserWorkItem(new WaitCallback(InventoryDirectory),new object[] { dirRoot, new AutoResetEvent(false) });

        int numActiveThreads = -1;
        while (numActiveThreads != 0)
        {
          Thread.Sleep(100);

          int maxThreads, availableWorkers, crap;
          ThreadPool.GetAvailableThreads(out availableWorkers, out crap);
          ThreadPool.GetMaxThreads(out maxThreads, out crap);

          numActiveThreads = maxThreads - availableWorkers;

          Console.CursorLeft = 0;
          Console.Write(string.Format("Dirs:[{0}]  Files:[{1}] Exceptions:[{2}] ThreadCount:[{3}]", _dirCount, _fileCount, _exceptionCount, numActiveThreads));
        }
      }
      catch (Exception ex)
      {
        Console.WriteLine(ex.ToString());
        LogException(ex.ToString());
      }
      finally
      {
        _fileStream.Flush();
        _directoryStream.Flush();
        _fileStream.Close();
        _directoryStream.Close();
      }
    }
    
    /// <summary>
    /// Walks through the contents of the folder and spawns additional threads for subdirectories.
    /// </summary>
    private static void InventoryDirectory(object input)
    {
      AutoResetEvent are = null;
      string dirPath = String.Empty;
      try
      {
        object[] inputArray = (object[]) input;

        dirPath = (string)inputArray[0];
        string[] dirs = Directory.GetDirectories(dirPath);

        are = (AutoResetEvent) inputArray[1];

        List<string> files = new List<string>();
        foreach (string file in Directory.GetFiles(dirPath))
        {
          RecordPath(file, true); 
        }

        for (int offset = 0; offset < dirs.Length; offset += MaxWaitHandles)
        {
          ProcessSubset(dirs, offset);
        }

        foreach (string childDir in dirs)
        {
          RecordPath(childDir, false); 
        }
      }
      catch (Exception ex)
      {
        LogException(dirPath + ex.ToString());
        Interlocked.Increment(ref _exceptionCount);
      }
      finally
      {
        if (are != null)
          are.Set();
      }
    }

    /// <summary>
    /// Queues a list of directories to be processed. This is necessary due to a 64 thread count limit
    /// imposed by ThreadPool
    /// </summary>
    /// <param name="dirs">Directory list.</param>
    /// <param name="offset">Offset, or 'place where we left off', for this subset.</param>
    private static void ProcessSubset(string[] dirs, int offset)
    {
      List<WaitHandle> waitHandles = new List<WaitHandle>();

      for (int idx=offset; idx < offset + MaxWaitHandles && idx < dirs.Length; idx++)
      {
        string childDir = dirs[idx];

        WaitHandle wh = new AutoResetEvent(false);
        waitHandles.Add(wh);
        ThreadPool.QueueUserWorkItem(new WaitCallback(InventoryDirectory), new object[] { childDir, wh });
      }
    }

    /// <summary>
    /// Record the path to file.
    /// </summary>
    /// <param name="path">Path to record.</param>
    /// <param name="isFile">Well, is it?</param>
    private static void RecordPath(string path, bool isFile)
    {
      try
      {
        if (isFile)
        {
          StringBuilder sb = new StringBuilder(path);
          sb.Append(",");
          FileInfo fi = new FileInfo(path);
          sb.Append(fi.Length);

          _fileFileWriter.WriteLine(sb.ToString());
          _fileFileWriter.Flush();
          Interlocked.Increment(ref _fileCount);
        }
        // its' a directory
        else
        {
          _dirFileWriter.WriteLine(path);
          _dirFileWriter.Flush();
          Interlocked.Increment(ref _dirCount);
        }
      }
      catch (Exception ex)
      {
        throw new Exception(string.Format("Operation failed. Path:{0} isFile:{1} Exception:{2}", path, isFile, ex));
      }
    }

    /// <summary>
    /// Log an exception by appending to exception file.
    /// </summary>
    private static void LogException(string exception)
    {
      try
      {
        lock (_exceptionFs)
        {
          StreamWriter sw = new StreamWriter(_exceptionFs);
          sw.AutoFlush = true;
          sw.WriteLine(exception);
          sw.Flush();
        }
      }
      catch (Exception ex)
      {
        // We don't really want to bomb out simply because we couldn't write to file - do we?
        Console.WriteLine("Failed to write to exception file: " + ex.ToString());
      }
    }
    #endregion // Private methods
  }
}
