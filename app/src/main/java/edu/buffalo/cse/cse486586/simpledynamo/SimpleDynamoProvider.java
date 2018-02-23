package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


public class SimpleDynamoProvider extends ContentProvider {
    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    public static List<String> nodeInformation = new ArrayList<>();
    public static List<String> messages = new ArrayList<>();
    public static Map<String,String> missedKeysMap = new HashMap<>();
    List<String> replicas = new ArrayList<>();
    int deletedMessagesCounter;
    int insertedMessagesCounter;
    Context context;
    String portStr;
    String localNodeID;
    boolean flag = false;
    boolean shouldConcatResult;
    boolean startRecover = false;
    boolean ifNodeFailed = false;
    String queryMessageFromOtherNode = null;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        try {
            if(selection.equals("@")){
                context = getContext();
                String storedFiles[] = context.fileList();
                for(int i=0; i<storedFiles.length; i++){
                    context.deleteFile(storedFiles[i]);
                }
                return 1;
            }
            else if(selection.equals("*")){
                Uri newUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                delete(newUri,"@",null);
                deletedMessagesCounter = 0;
                for(int i=0; i< SimpleDynamoProvider.nodeInformation.size(); i++) {
                    if (!SimpleDynamoProvider.nodeInformation.get(i).equals(portStr)) {
                        String port = SimpleDynamoProvider.nodeInformation.get(i);
                        String portNumber = Integer.toString(Integer.parseInt(port) * 2);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "3", portNumber);
                    }
                }
                while(!(deletedMessagesCounter==4) && ((!ifNodeFailed) || (!(deletedMessagesCounter==3)))){
                    Thread.sleep(1);
                }
                return 1;
            }
            else{
                String key = this.genHash(selection);
                String realCoordinator = this.findRealCoordinator(key);
                deletedMessagesCounter = 0;
                if (realCoordinator.equals(portStr)) {
                    this.deleteGivenFileFromLocal(selection);
                    for(int i=0; i< replicas.size(); i++){
                        String portNumber = Integer.toString(Integer.parseInt(replicas.get(i))*2);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "2", portNumber+","+selection);     //forwarding insert request to replicas
                    }
                    while(!(deletedMessagesCounter==2) && ((!ifNodeFailed) || (!(deletedMessagesCounter==1)))){
                        Thread.sleep(1);
                    }
                    return 1;
                }
                else{
                    List<String> replicas = this.getReplicas(realCoordinator);
                    for(int i=0; i<replicas.size(); i++){
                        String currentNode = replicas.get(i);
                        String portNumber = Integer.toString(Integer.parseInt(currentNode)*2);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "2", portNumber+","+selection);     //forwarding insert request to replicas
                    }
                    String portNumber = Integer.toString(Integer.parseInt(realCoordinator)*2);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "2", portNumber+","+selection);     //forwarding delete request to appropriate client

                    while(!(deletedMessagesCounter==3) && ((!ifNodeFailed) || (!(deletedMessagesCounter==2)))){
                        Thread.sleep(1);
                    }
                    return 1;
                }
            }
        }
        catch(Exception exception){
            exception.printStackTrace();
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        context = getContext();
        String fileName = values.getAsString("key");
        String value = values.getAsString("value");

        try {
            String key = this.genHash(fileName);
            String realCoordinator = this.findRealCoordinator(key);
            insertedMessagesCounter = 0;
            if(realCoordinator.equals(portStr)){
                this.writeOneValueToFile(fileName, value);
                for(int i=0; i< replicas.size(); i++){
                    String portNumber = Integer.toString(Integer.parseInt(replicas.get(i))*2);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "1", portNumber+","+fileName+","+value);     //forwarding insert request to replicas
                }
                while(!(insertedMessagesCounter==2) && ((!ifNodeFailed) || (!(insertedMessagesCounter==1)))){
                    Thread.sleep(1);
                }
            }
            else{
                List<String> replicasOfRealCoordinator = this.getReplicas(realCoordinator);
                for(int i=0; i<replicasOfRealCoordinator.size(); i++){
                    String currentNode = replicasOfRealCoordinator.get(i);
                    String portNumber = Integer.toString(Integer.parseInt(currentNode)*2);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "1", portNumber+","+fileName+","+value);     //forwarding insert request to replicas
                }
                String portNumber = Integer.toString(Integer.parseInt(realCoordinator)*2);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "1", portNumber+","+fileName+","+value);     //forwarding insert request to appropriate client
                while(!(insertedMessagesCounter==3) && ((!ifNodeFailed) || (!(insertedMessagesCounter==2)))){
                    Thread.sleep(1);
                }
            }
        }
        catch (Exception exception) {
            exception.printStackTrace();
        }
        Log.v("insert", values.toString());
        return uri;
    }

    @Override
    public synchronized boolean onCreate() {
        context = getContext();
        TelephonyManager tel = (TelephonyManager) context.getSystemService(context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        try {
            localNodeID = genHash(portStr);
            nodeInformation.addAll(Arrays.asList("5562", "5556", "5554", "5558", "5560"));
            replicas = getReplicas(portStr);

            missedKeysMap.clear();
            String storedFiles[];
            synchronized (this) {
                context = getContext();
                storedFiles = context.fileList();
            }

//			Uri newUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
//			delete(newUri,"@",null);

            if (storedFiles.length > 0) {
                startRecover = false;
                String predecessorOfCurrentNode = this.getFinalPredecessor(portStr);
                String successorOfCurrentNode = this.getFinalSuccessor(portStr);
                String portNumberPred = Integer.toString(Integer.parseInt(predecessorOfCurrentNode) * 2);
                String portNumberSucc = Integer.toString(Integer.parseInt(successorOfCurrentNode) * 2);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "6", portNumberPred);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "7", portNumberSucc + "," + portStr);

                while (!startRecover) {
                    Thread.sleep(1);
                }
                if (startRecover) {
                    Iterator<String> missedKeyMapIterator = SimpleDynamoProvider.missedKeysMap.keySet().iterator();
                    while (missedKeyMapIterator.hasNext()) {
                        int i;
                        String currentKey = missedKeyMapIterator.next();
                        String message = SimpleDynamoProvider.missedKeysMap.get(currentKey);
                        String valueInMap[] = message.split(",");
                        String storedVersion = "";
                        boolean existsInContext = false;
                        for (i = 0; i < storedFiles.length; i++) {
                            if (storedFiles[i].equals(currentKey)) {
                                existsInContext = true;
                                BufferedReader bufferedReader = getReader(storedFiles[i]);
                                String retrievedString = bufferedReader.readLine();
                                String storedValue[] = retrievedString.split(",");
                                storedVersion = storedValue[1];
                                if ((storedValue[0]).equals(valueInMap[0])) {
                                    break;
                                }
                            }
                        }
                        if (i == storedFiles.length) {
                            if ((!existsInContext) || ((Integer.parseInt(storedVersion) < Integer.parseInt(valueInMap[1])) && existsInContext)) {
                                FileOutputStream fileOutputStream = context.openFileOutput(currentKey, Context.MODE_PRIVATE);
                                fileOutputStream.write(message.getBytes());
                                fileOutputStream.close();
                            }
                        }
                    }
                }
            }
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort);
        }
        catch(Exception exception){
            exception.printStackTrace();
        }
        return true;
    }

    @Override
    public synchronized Cursor query(Uri uri, String[] projection, String selection,
                                     String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        context = getContext();
        String[] columnNames = {"key", "value"};
        MatrixCursor matrixCursor = new MatrixCursor(columnNames);
        String storedFiles[] = context.fileList();
        try {
            if (selection.equals("@")) {
                for (int i = 0; i < storedFiles.length; i++) {
                    Uri newUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                    MatrixCursor matrixCursorNext = (MatrixCursor) query(newUri, null, storedFiles[i], null, null);
                    if(!matrixCursorNext.isFirst()){
                        matrixCursorNext.moveToFirst();
                    }
                    String value = matrixCursorNext.getString(1);

                    Object[] columnvalues = {storedFiles[i], value};
                    if (columnvalues.length == columnNames.length) {
                        matrixCursor.addRow(columnvalues);
                    }
                }
            } else if (selection.equals("*")) {
                String key;
                String returnQueryMessage = "";

                for (int i = 0; i < storedFiles.length; i++) {
                    BufferedReader bufferedReader = getReader(storedFiles[i]);
                    String retrievedString = bufferedReader.readLine();
                    returnQueryMessage = returnQueryMessage.concat(storedFiles[i] + "%" + retrievedString + "$");
                }
                String finalStarResult = returnQueryMessage;
                for (int i = 0; i < SimpleDynamoProvider.nodeInformation.size(); i++) {
                    if (!SimpleDynamoProvider.nodeInformation.get(i).equals(portStr)) {
                        String port = SimpleDynamoProvider.nodeInformation.get(i);
                        String portNumber = Integer.toString(Integer.parseInt(port) * 2);
                        shouldConcatResult = false;
                        ifNodeFailed = false;
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "4", portNumber);     //forwarding * query request to appropriate client
                        while ((!shouldConcatResult) && (!ifNodeFailed)) {
                            Thread.sleep(1);
                        }
                        if (shouldConcatResult) {
                            finalStarResult = finalStarResult.concat(queryMessageFromOtherNode);
                        }
                    }
                }

                int length = finalStarResult.length();
                if (length != 0) {
                    String finalStarResultModified = finalStarResult.substring(0, length - 1) + finalStarResult.substring(length);
                    Map<String,String> starKeysMap = new HashMap<>();
                    String queryResult[] = finalStarResultModified.split("\\$");
                    for (int i = 0; i < queryResult.length; i++) {
                        String keyValuePairs[] = queryResult[i].split("\\%");
                        if(starKeysMap.containsKey(keyValuePairs[0])) {
                            String storedValue[] = keyValuePairs[1].split(",");
                            String mapValue = starKeysMap.get(keyValuePairs[0]);
                            String storedMapValueSplit[] = mapValue.split(",");
                            if(Integer.parseInt(storedValue[1]) > Integer.parseInt(storedMapValueSplit[1])){
                                starKeysMap.remove(keyValuePairs[0]);
                                starKeysMap.put(keyValuePairs[0],keyValuePairs[1]);
                            }
                        }
                        else{
                            starKeysMap.put(keyValuePairs[0], keyValuePairs[1]);
                        }
                    }

                    Iterator<String> starMapIterator = starKeysMap.keySet().iterator();
                    while (starMapIterator.hasNext()){
                        String keyInMap = starMapIterator.next();
                        String valueInMap = starKeysMap.get(keyInMap);
                        String finalValueToBeReturned[] = valueInMap.split(",");
                        Object[] columnvalues = {keyInMap, finalValueToBeReturned[0]};
                        if (columnvalues.length == columnNames.length) {
                            matrixCursor.addRow(columnvalues);
                        }
                    }
                }
            } else {
                String key = this.genHash(selection);
                String realCoordinator = this.findRealCoordinator(key);
                List<String> comparisionPorts = new ArrayList<>();

                SimpleDynamoProvider.messages.clear();
                if (realCoordinator.equals(portStr)) {
                    String message = readRecentVersionFromFile(selection);
                    SimpleDynamoProvider.messages.add(message);
                    comparisionPorts = replicas;
                } else {
                    flag = false;
                    boolean isReplica = checkIfCurrentNodeIsReplicaOfRealCoordinator(realCoordinator);
                    List<String> replicas = getReplicas(realCoordinator);
                    if (isReplica) {
                        String message = readRecentVersionFromFile(selection);
                        SimpleDynamoProvider.messages.add(message);
                        for (int i = 0; i < replicas.size(); i++) {
                            if (!replicas.get(i).equals(portStr)) {
                                comparisionPorts.add(replicas.get(i));
                            }
                        }
                        comparisionPorts.add(realCoordinator);
                    } else {
                        comparisionPorts = replicas;
                        comparisionPorts.add(realCoordinator);
                    }
                }
                ifNodeFailed = false;

                for (int j = 0; j < comparisionPorts.size(); j++) {
                    String portNumber = Integer.toString(Integer.parseInt(comparisionPorts.get(j)) * 2);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "5", portNumber + "," + selection + "," + "a");     //forwarding insert request to replicas
                }

                while ((!(SimpleDynamoProvider.messages.size() == 3)) && ((!ifNodeFailed) || (!(SimpleDynamoProvider.messages.size()==2)))) {
                    Thread.sleep(1);
                }

                String finalValue = this.getFinalReadValue();
                Object[] columnvalues = {selection, finalValue};
                if (columnvalues.length == columnNames.length) {
                    matrixCursor.addRow(columnvalues);
                }
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        Log.v("query", selection);
        return matrixCursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public String getFinalSuccessor(String portStr){
        String successorPort = null;
        if(portStr.equals("5560")){
            successorPort = "5562";
        }
        else{
            boolean assignSuccessor = false;
            for (int i=0; i<SimpleDynamoProvider.nodeInformation.size(); i++) {
                if(assignSuccessor){
                    successorPort = SimpleDynamoProvider.nodeInformation.get(i);
                    break;
                }
                if (SimpleDynamoProvider.nodeInformation.get(i).equals(portStr)) {
                    assignSuccessor = true;
                }
            }
        }
        return successorPort;
    }

    public String getFinalPredecessor(String portStr){

        String predecessorPort = null;
        if(portStr.equals("5562")){
            predecessorPort = "5560";
        }
        else{
            boolean assignPredecessor = false;
            for (int i=SimpleDynamoProvider.nodeInformation.size()-1; i>=0; i--) {
                if(assignPredecessor){
                    predecessorPort = SimpleDynamoProvider.nodeInformation.get(i);
                    break;
                }
                if (SimpleDynamoProvider.nodeInformation.get(i).equals(portStr)) {
                    assignPredecessor = true;
                }
            }
        }
        return  predecessorPort;
    }

    public  List<String> getReplicas(String portStr){
        List<String> replicas = new ArrayList<>();
        if(portStr.equals("5558")){
            replicas.addAll(Arrays.asList("5560","5562"));
        }
        else if(portStr.equals("5560")){
            replicas.addAll(Arrays.asList("5562","5556"));
        }
        else {
            boolean generateReplicas = false;
            int count =0;
            for (int i=0; i<SimpleDynamoProvider.nodeInformation.size();i++) {
                if (SimpleDynamoProvider.nodeInformation.get(i).equals(portStr)) {
                    generateReplicas = true;
                }
                if(generateReplicas && !SimpleDynamoProvider.nodeInformation.get(i).equals(portStr)) {
                    if (count < 2) {
                        replicas.add(SimpleDynamoProvider.nodeInformation.get(i));
                        count++;
                        if(count==2){
                            break;
                        }
                    }
                }
            }
        }
        return replicas;
    }

    public String findRealCoordinator(String key){
        for (int i=0; i<SimpleDynamoProvider.nodeInformation.size(); i++) {
            String currentNodePort = SimpleDynamoProvider.nodeInformation.get(i);
            String predecessor = this.getFinalPredecessor(currentNodePort);
            try {
                if ((key.compareTo(genHash(currentNodePort)) <= 0 && key.compareTo(genHash(predecessor)) > 0)
                        || (genHash(predecessor).compareTo(genHash(currentNodePort)) > 0 && (key.compareTo(genHash(predecessor)) > 0 || key.compareTo(genHash(currentNodePort)) < 0))) {
                    return currentNodePort;
                }
            } catch (Exception exception) {
                exception.printStackTrace();
            }
        }
        return null;
    }

    public String getFinalReadValue(){
        String finalValue = null;
        try {
            String receivedMessage[] = (messages.get(0)).split("\\,");
            String receivedMessage1[] = (messages.get(1)).split("\\,");
            if ((ifNodeFailed)) {
                finalValue = Integer.parseInt(receivedMessage[1]) > Integer.parseInt(receivedMessage1[1]) ? receivedMessage[0] : receivedMessage1[0];
            }
            else {
                String receivedMessage2[] = (SimpleDynamoProvider.messages.get(2)).split("\\,");
                int max = Math.max(Math.max(Integer.parseInt(receivedMessage[1]), Integer.parseInt(receivedMessage1[1])), Integer.parseInt(receivedMessage2[1]));
                if(max == Integer.parseInt(receivedMessage[1])){
                    finalValue = receivedMessage[0];
                }
                else if (max == Integer.parseInt(receivedMessage1[1])){
                    finalValue = receivedMessage1[0];
                }
                else{
                    finalValue = receivedMessage2[0];
                }
            }
            SimpleDynamoProvider.messages.clear();
        }
        catch(Exception exception){
            exception.printStackTrace();
        }
        return finalValue;
    }

    public boolean checkIfCurrentNodeIsReplicaOfRealCoordinator(String realCoordinator){
        List<String> replicas = this.getReplicas(realCoordinator);
        for(int i=0; i< replicas.size(); i++){
            if(replicas.get(i).equals(portStr)){
                return true;
            }
        }
        return false;
    }

    public void addToRecoveryMap(String missedKeyValuePairs){
        int length = missedKeyValuePairs.length();
        if (length != 0) {
            String finalResultModified = missedKeyValuePairs.substring(0, length - 1) + missedKeyValuePairs.substring(length);
            String queryResult[] = finalResultModified.split("\\$");
            for (int i = 0; i < queryResult.length; i++) {
                String keyValueVersionPairs[] = queryResult[i].split("\\%");
                SimpleDynamoProvider.missedKeysMap.put(keyValueVersionPairs[0], keyValueVersionPairs[1]);
            }
        }
    }

    public String readRecentVersionFromFile(String fileName){
        context = getContext();
        String storedFiles[] = context.fileList();
        String message = ""+","+"-1";
        int version = -1;
        try {
            for (int i = 0; i < storedFiles.length; i++) {
                if (storedFiles[i].equals(fileName)) {
                    BufferedReader bufferedReader = getReader(storedFiles[i]);
                    String retrievedString = bufferedReader.readLine();
                    String storedValue[] = retrievedString.split(",");
                    int currentVersion = Integer.parseInt(storedValue[1]);
                    if(currentVersion > version){
                        version = currentVersion;
                        message = retrievedString;
                    }
                }
            }
        }
        catch(Exception exception){
            System.out.print(exception.getMessage());
        }
        return message;
    }

    public int detemineVersion(String fileName){
        int version = 0;
        String message = readRecentVersionFromFile(fileName);
        String receivedvalue[] = message.split(",");
        if(!(receivedvalue[0].equals("")) && !(Integer.parseInt(receivedvalue[1])==-1)){
            version = (Integer.parseInt(receivedvalue[1]))+1;
        }
        return version;
    }

    public void writeOneValueToFile(String fileName, String value){
        try {
            context = getContext();
            int newVersion = detemineVersion(fileName);
            String finalValue = value+","+newVersion;
            FileOutputStream fileOutputStream = context.openFileOutput(fileName, Context.MODE_PRIVATE);
            fileOutputStream.write(finalValue.getBytes());
            fileOutputStream.close();
        } catch(Exception exception){
            System.out.print(exception.getMessage());
        }
    }

    public void deleteGivenFileFromLocal(String selection){
        context = getContext();
        String storedFiles[] = context.fileList();
        for (int i = 0; i < storedFiles.length; i++) {
            if (storedFiles[i].equals(selection)) {
                context.deleteFile(selection);
                break;
            }
        }
    }

    public static Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    public BufferedReader getReader(String key) throws Exception{
        context = getContext();
        FileInputStream fileInputStream = context.openFileInput(key);
        InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
        fileInputStream.close();
        return new BufferedReader(inputStreamReader);
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            try {
                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    clientSocket.setSoTimeout(1000);
                    DataInputStream dataInputStream = new DataInputStream(clientSocket.getInputStream());
                    DataOutputStream dataOutputStream = new DataOutputStream(clientSocket.getOutputStream());
                    String message[] = dataInputStream.readUTF().split(",");
                    context = getContext();
                    String storedFiles[] = context.fileList();

                    if (message[0].equals("1")) {
                        writeOneValueToFile(message[1], message[2]);
                        dataOutputStream.writeUTF("Insertion on replicas done");
                    }
                    else if (message[0].equals("2")) {
                        deleteGivenFileFromLocal(message[1]);
                        dataOutputStream.writeUTF("Delete on replicas done");
                    }
                    else if (message[0].equals("3")) {
                        Uri newUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                        int r = delete(newUri, "@", null);
                        if (r == 1) {
                           dataOutputStream.writeUTF("Delete operation done");
                        }
                    }
                    else if (message[0].equals("4")) {
                        String reply = "";
                        try {
                            for (int i = 0; i < storedFiles.length; i++) {
                                BufferedReader bufferedReader = getReader(storedFiles[i]);
                                String retrievedString = bufferedReader.readLine();
                                reply = reply.concat(storedFiles[i] + "%" + retrievedString + "$");
                            }
                            dataOutputStream.writeUTF("Query result generated!");
                            dataOutputStream.writeUTF(reply);
                        }
                        catch(Exception exception){
                            exception.getMessage();
                        }
                    }
                    else if(message[0].equals("5")) {
                        String newMessage = readRecentVersionFromFile(message[1]);
                        dataOutputStream.writeUTF(newMessage);
                    }
                    else if(message[0].equals("6")) {
                        String myPredecessor = getFinalPredecessor(portStr);
                        String missedKeyValuePairs = "";
                        try {
                            for (int i = 0; i < storedFiles.length; i++) {
                                String key = genHash(storedFiles[i]);
                                String realCoordinator = findRealCoordinator(key);
                                if ( realCoordinator.equals(portStr) || realCoordinator.equals(myPredecessor) ) {
                                    BufferedReader bufferedReader = getReader(storedFiles[i]);
                                    String storedValue[] = bufferedReader.readLine().split(",");
                                    missedKeyValuePairs = missedKeyValuePairs.concat(storedFiles[i] + "%" + storedValue[0] + "," + storedValue[1] + "$");
                                }
                            }
                            dataOutputStream.writeUTF("here are missed keys");
                            dataOutputStream.writeUTF(missedKeyValuePairs);
                        }
                        catch(Exception exception){
                            System.out.print(exception.getMessage());
                        }
                    }
                    else if(message[0].equals("7")) {
                        String missedKeyValuePairs = "";
                        try {
                            for (int i = 0; i < storedFiles.length; i++) {
                                String key = genHash(storedFiles[i]);
                                String realCoordinator = findRealCoordinator(key);
                                if (realCoordinator.equals(message[1])) {
                                    BufferedReader bufferedReader = getReader(storedFiles[i]);
                                    String storedValue[] = bufferedReader.readLine().split(",");
                                    missedKeyValuePairs = missedKeyValuePairs.concat(storedFiles[i] + "%" + storedValue[0] + "," + storedValue[1] + "$");
                                }
                            }
                            dataOutputStream.writeUTF("here are missed keys");
                            dataOutputStream.writeUTF(missedKeyValuePairs);
                        }
                        catch(Exception exception){
                            System.out.print(exception.getMessage());
                        }
                    }
                    clientSocket.close();
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch(SocketTimeoutException e){
                Log.e(TAG, "ClientTask SocketTimeOutException"+e);
            } catch (StreamCorruptedException e) {
                Log.e(TAG, "ClientTask StreamCorruptedException"+e);
            }catch (FileNotFoundException e) {
                Log.e(TAG, "ClientTask FileNotFoundException"+e);
            }catch (EOFException e) {
                Log.e(TAG, "ClientTask EOFException"+e);
            }catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException"+e);
            }
            return null;
        }

    }


    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                String message = "";
                String msgType = msgs[0];
                if(msgs.length==2) {
                    message = msgs[1];
                }
                String recMessage[] = message.split(",");
                String portNumber = recMessage[0];
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(portNumber));
                DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());

                //forward insert request to replicas
                if(msgType.equals("1")){
                    dataOutputStream.writeUTF(msgType+","+recMessage[1]+","+recMessage[2]);
                    if(dataInputStream.readUTF().equals("Insertion on replicas done")){
                        insertedMessagesCounter++;
                    }
                }
                else if(msgType.equals("2")){
                    dataOutputStream.writeUTF(msgType+","+recMessage[1]);
                    if(dataInputStream.readUTF().equals("Delete on replicas done")){
                        deletedMessagesCounter++;
                    }
                }
                //forward delete * request to a port (delete all)
                else if(msgType.equals("3")){
                    dataOutputStream.writeUTF(msgType);
                    if(dataInputStream.readUTF().equals("Delete operation done")){
                        deletedMessagesCounter++;
                    }
                }
                //forward query * request to a node (query all)
                else if(msgType.equals("4")){
                    dataOutputStream.writeUTF(msgType);
                    if(dataInputStream.readUTF().equals("Query result generated!")){
                        queryMessageFromOtherNode = dataInputStream.readUTF();
                        shouldConcatResult = true;
                    }
                }
                else if(msgType.equals("5")) {
                    dataOutputStream.writeUTF(msgType + "," + recMessage[1]);
                    SimpleDynamoProvider.messages.add(dataInputStream.readUTF());
                }
                else if(msgType.equals("6")){
                    dataOutputStream.writeUTF(msgType);
                    if(dataInputStream.readUTF().equals("here are missed keys")){
                        addToRecoveryMap(dataInputStream.readUTF());
                    }
                }
                else if(msgType.equals("7")){
                    dataOutputStream.writeUTF(msgType+","+recMessage[1]);
                    if(dataInputStream.readUTF().equals("here are missed keys")){
                        addToRecoveryMap(dataInputStream.readUTF());
                        startRecover = true;
                    }
                }
                socket.close();
            }
            catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
                ifNodeFailed = true;
            }catch(SocketTimeoutException e){
                Log.e(TAG, "ClientTask SocketTimeOutException"+e);
                ifNodeFailed = true;
            } catch (StreamCorruptedException e) {
                Log.e(TAG, "ClientTask StreamCorruptedException"+e);
                ifNodeFailed = true;
            }catch (FileNotFoundException e) {
                Log.e(TAG, "ClientTask FileNotFoundException"+e);
                ifNodeFailed = true;
            }catch (EOFException e) {
                Log.e(TAG, "ClientTask EOFException"+e);
                ifNodeFailed = true;
            }catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException"+e);
                ifNodeFailed = true;
            }
            return null;
        }
    }
}
