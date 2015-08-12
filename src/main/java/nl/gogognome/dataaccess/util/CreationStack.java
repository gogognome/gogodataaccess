package nl.gogognome.dataaccess.util;

public class CreationStack {

    private StackTraceElement[] creationStack;

    public CreationStack() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        int startIndex = 2; // skip getStackTrace and this constructor
        creationStack = new StackTraceElement[Math.min(10, stackTrace.length) - startIndex];
        System.arraycopy(stackTrace, startIndex, creationStack, 0, creationStack.length);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder(1000);
        for (StackTraceElement stackTraceElement : creationStack) {
            sb.append(stackTraceElement.getClassName()).append('.').append(stackTraceElement.getMethodName()).append(" (");
            sb.append(stackTraceElement.getLineNumber()).append(")").append('\n');
        }
        return sb.toString();
    }

}
