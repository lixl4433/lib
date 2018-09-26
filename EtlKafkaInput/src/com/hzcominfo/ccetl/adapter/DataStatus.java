/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hzcominfo.ccetl.adapter;

import java.util.List;

public class DataStatus {

	public long config_id = 0;
	public long lastTime = 0;
	public long size = 0;
	public long timestamp = 0;
	public String fileName = "";
	public int dealedFlag = 0;//是否已经处理过
	public int errorCount = 0;
	public int record = 0;
	public List<String> fileContent;
	public AdapterDataOut ado;
}
