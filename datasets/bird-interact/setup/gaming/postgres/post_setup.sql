ALTER TABLE ONLY public.audioandmedia
ADD CONSTRAINT audioandmedia_pkey PRIMARY KEY (audregistry);


ALTER TABLE ONLY public.deviceidentity
ADD CONSTRAINT deviceidentity_pkey PRIMARY KEY (devregistry);


ALTER TABLE ONLY public.interactionandcontrol
ADD CONSTRAINT interactionandcontrol_pkey PRIMARY KEY (interactregistry);


ALTER TABLE ONLY public.mechanical
ADD CONSTRAINT mechanical_pkey PRIMARY KEY (mechregistry);


ALTER TABLE ONLY public.performance
ADD CONSTRAINT performance_pkey PRIMARY KEY (perfregistry);


ALTER TABLE ONLY public.physicaldurability
ADD CONSTRAINT physicaldurability_pkey PRIMARY KEY (physregistry);


ALTER TABLE ONLY public.rgb
ADD CONSTRAINT rgb_pkey PRIMARY KEY (rgbregistry);


ALTER TABLE ONLY public.testsessions
ADD CONSTRAINT testsessions_pkey PRIMARY KEY (sessionregistry);


ALTER TABLE ONLY public.audioandmedia
ADD CONSTRAINT audiodevfk FOREIGN KEY (auddevref) REFERENCES public.deviceidentity(devregistry);


ALTER TABLE ONLY public.audioandmedia
ADD CONSTRAINT audioperffk FOREIGN KEY (audperfref) REFERENCES public.performance(perfregistry);


ALTER TABLE ONLY public.deviceidentity
ADD CONSTRAINT devsessionfk FOREIGN KEY (devsessionref) REFERENCES public.testsessions(sessionregistry);


ALTER TABLE ONLY public.interactionandcontrol
ADD CONSTRAINT interactdevfk FOREIGN KEY (interactdevref) REFERENCES public.deviceidentity(devregistry);


ALTER TABLE ONLY public.interactionandcontrol
ADD CONSTRAINT interactphysfk FOREIGN KEY (interactphysref) REFERENCES public.physicaldurability(physregistry);


ALTER TABLE ONLY public.mechanical
ADD CONSTRAINT mechdevfk FOREIGN KEY (mechdevref) REFERENCES public.deviceidentity(devregistry);


ALTER TABLE ONLY public.mechanical
ADD CONSTRAINT mechperffk FOREIGN KEY (mechperfref) REFERENCES public.performance(perfregistry);


ALTER TABLE ONLY public.performance
ADD CONSTRAINT perfsessionfk FOREIGN KEY (perfsessionref) REFERENCES public.testsessions(sessionregistry);


ALTER TABLE ONLY public.physicaldurability
ADD CONSTRAINT physperffk FOREIGN KEY (physperfref) REFERENCES public.performance(perfregistry);


ALTER TABLE ONLY public.physicaldurability
ADD CONSTRAINT physrgbfk FOREIGN KEY (physrgbref) REFERENCES public.rgb(rgbregistry);


ALTER TABLE ONLY public.rgb
ADD CONSTRAINT rgbaudfk FOREIGN KEY (rgbaudref) REFERENCES public.audioandmedia(audregistry);


ALTER TABLE ONLY public.rgb
ADD CONSTRAINT rgbmechfk FOREIGN KEY (rgbmechref) REFERENCES public.mechanical(mechregistry);


GRANT ALL ON SCHEMA public TO PUBLIC;

