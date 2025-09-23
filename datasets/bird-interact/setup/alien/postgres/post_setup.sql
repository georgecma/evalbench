
ALTER TABLE ONLY public.signaladvancedphenomena
    ADD CONSTRAINT fk_advanced_signal FOREIGN KEY (signalref) REFERENCES public.signals(signalregistry);


ALTER TABLE ONLY public.signalclassification
    ADD CONSTRAINT fk_classification_signal FOREIGN KEY (signalref) REFERENCES public.signals(signalregistry);


ALTER TABLE ONLY public.signaldecoding
    ADD CONSTRAINT fk_decoding_signal FOREIGN KEY (signalref) REFERENCES public.signals(signalregistry);


ALTER TABLE ONLY public.signaldynamics
    ADD CONSTRAINT fk_dynamics_signal FOREIGN KEY (signalref) REFERENCES public.signals(signalregistry);


ALTER TABLE ONLY public.observationalconditions
    ADD CONSTRAINT fk_observcond_signalsref FOREIGN KEY (signalref) REFERENCES public.signals(signalregistry);


ALTER TABLE ONLY public.signalprobabilities
    ADD CONSTRAINT fk_probabilities_signal FOREIGN KEY (signalref) REFERENCES public.signals(signalregistry);


ALTER TABLE ONLY public.researchprocess
    ADD CONSTRAINT fk_researchprocess_signalsref FOREIGN KEY (signalref) REFERENCES public.signals(signalregistry);


ALTER TABLE ONLY public.signals
    ADD CONSTRAINT fk_signals_telescopesref FOREIGN KEY (telescref) REFERENCES public.telescopes(telescregistry);


ALTER TABLE ONLY public.sourceproperties
    ADD CONSTRAINT fk_source_signal FOREIGN KEY (signalref) REFERENCES public.signals(signalregistry);


ALTER TABLE ONLY public.telescopes
    ADD CONSTRAINT fk_telescopes_observatoriesref FOREIGN KEY (observstation) REFERENCES public.observatories(observstation);
