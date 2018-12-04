package cn.py.trident;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class NameFilter extends BaseFilter{

	@Override
	public boolean isKeep(TridentTuple tuple) {
		String name = tuple.getStringByField("name");
		if("tom".equals(name)){
			//�������Ϊtom,��ѡ����ˣ�������tuple������
			return false;
		}else {
			return true;
		}
	}

}
