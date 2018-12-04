package cn.py.trident;

import java.util.Iterator;

import backtype.storm.tuple.Fields;
import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 * 接受上游发来的tuple，并打印所有数据
 * @author pyang
 *
 */
public class PringFilter extends BaseFilter{
	
	@Override
	public boolean isKeep(TridentTuple tuple) {
//		String name = tuple.getStringByField("name");
//		int age = tuple.getIntegerByField("age");
//		System.out.println(name+":"+age);
		
		Fields keys = tuple.getFields();
		Iterator<String> it = keys.iterator();
		while(it.hasNext()){
			String key = it.next();
			Object value = tuple.getValueByField(key);
			System.out.println(key+":"+value);
		}
		return false;
	}

}
