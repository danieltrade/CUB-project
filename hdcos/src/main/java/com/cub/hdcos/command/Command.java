package com.cub.hdcos.command;

import com.cub.hdcos.exception.CUBException;

public interface Command {
	
	public void execute() throws CUBException;
}
