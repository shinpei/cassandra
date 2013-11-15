package org.apache.cassandra.utils;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;

import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ColumnAlias {
    private static final Map<ByteBuffer, Integer> fMap = new HashMap<ByteBuffer, Integer>();
    private static final Map<Integer, ByteBuffer> rMap = new HashMap<Integer, ByteBuffer>();
    private static int lastIndex = 0x0;

    /*public static int aliasLength(ByteBuffer name){
        int index = Integer.MIN_VALUE;
        if (fMap.containsKey(name)){
            index = fMap.get(name);
            return VlqCoder.size(index);
        }
        int size = name.remaining();
        size += VlqCoder.size(size);
        return size;
    }
    */

    public static int addAlias(String name) {
        lastIndex += 3;
        fMap.put(ByteBuffer.wrap(name.getBytes()), lastIndex);
        rMap.put(lastIndex, ByteBuffer.wrap(name.getBytes()));
        return lastIndex;
    }
    public static int aliasSize(ByteBuffer name) {
        if (fMap.containsKey(name)) {
            //int index = fMap.get(name);
            //return VlqCoder.sizeo(index);
            return 2;
        } else return name.remaining();

    }
    public static boolean contains(ByteBuffer name) {
        return fMap.containsKey(name);
    }
    public static boolean alias(ByteBuffer name, int flags, DataOutput dos){
        int index = Integer.MIN_VALUE;
        if (fMap.containsKey(name)){
            try {
                flags |= ColumnSerializer.ALIAS_MASK;
                dos.writeByte(flags);
                index = fMap.get(name);
                dos.writeShort(index);
                return true;
            }
            catch (ClassCastException e){e.printStackTrace();}
            catch (IOException ioe) {ioe.printStackTrace(); }
            catch(NullPointerException e){e.printStackTrace();}
        } else {
            try {
                dos.writeByte(flags);
                ByteBufferUtil.writeWithShortLength(name, dos);
            } catch (IOException ioe) { ioe.printStackTrace(); }
            return false;
        }
        return false;
    }

    public static ByteBuffer unalias(DataInput dis){
        try
           {
            int anum = (int) dis.readShort();
                ByteBuffer aname = null;
                try{
                    aname = rMap.get(anum);
                }
                catch (ClassCastException e){ e.printStackTrace();}
                catch(NullPointerException e){e.printStackTrace();}

                if(aname != null)
                    return aname;
                else
                    throw new RuntimeException("Could not unalias column name, alias id=" + anum);

        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
