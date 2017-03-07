package com.cub.hdcos.command;

import java.util.ArrayList;
import java.util.List;

import com.cub.hdcos.exception.CUBException;

public class MultiHdcosCommand implements Command {
	
	private List<HqlCommand> hqlCommands = new ArrayList<HqlCommand>(); 
	
	public void addCommand(HqlCommand hqlCommands) {
		this.hqlCommands.add(hqlCommands);
	}
	
	public void clearCommand() {
		hqlCommands.clear();
	}
	
	@Override
	public void execute() throws CUBException {
		for(HqlCommand command : hqlCommands) {
			command.executeHQL();
		}
	}
}
