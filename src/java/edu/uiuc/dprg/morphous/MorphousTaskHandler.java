package edu.uiuc.dprg.morphous;

import edu.uiuc.dprg.morphous.MorphousTaskMessageSender.MorphousTask;
import edu.uiuc.dprg.morphous.MorphousTaskMessageSender.MorphousTaskResponse;

public interface MorphousTaskHandler {
	public MorphousTaskResponse handle(MorphousTask task);
}
