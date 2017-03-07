package com.cub.hdcos.command;

import com.cub.hdcos.exception.CUBException;

public class HdcosCommand implements Command {
	
	HqlCommand hqlCommand;
	
	public HdcosCommand(HqlCommand hqlCommand) {
		this.hqlCommand = hqlCommand;
	}
	
	@Override
	public void execute() throws CUBException {
		hqlCommand.executeHQL();
	}

}
