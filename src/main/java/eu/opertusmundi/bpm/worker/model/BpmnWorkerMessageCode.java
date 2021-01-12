package eu.opertusmundi.bpm.worker.model;

import eu.opertusmundi.common.model.MessageCode;

public enum BpmnWorkerMessageCode implements MessageCode {
    UNKNOWN,
    VARIABLE_NOT_FOUND,
    INVALID_VARIABLE_VALUE,
    ;

    @Override
    public String key() {
        return this.getClass().getSimpleName() + '.' + this.name();
    }

}
