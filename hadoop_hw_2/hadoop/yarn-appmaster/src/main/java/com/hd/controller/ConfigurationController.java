package com.hd.controller;

import javax.validation.Valid;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.hd.form.ConfigurationForm;
import com.hd.master.CustomApplicationMaster;

@Controller
public class ConfigurationController {

    public static Logger LOG =
        LoggerFactory.getLogger(ConfigurationController.class);
    @Autowired
    private ApplicationContext context;

    @RequestMapping(value = "/", method = RequestMethod.GET)
    public String processGetMethod(@Valid ConfigurationForm post, Model model) {

        LOG.info(
            "Init page with configuration containers [cores:{}, memory:{}, priority:{}, containers:{}]",
            post.getCores(), post.getMemory(), post.getPriority(),
            post.getNumberOfContainers());

        model.addAttribute("settings", ConfigurationForm.createDefault());

        return "settings";
    }

    @RequestMapping(value = "/", method = RequestMethod.POST)
    public String processPostMethod(@ModelAttribute("settings") @Valid ConfigurationForm post,
        BindingResult bindingResult, Model model) {

        int memory = post.getMemory();
        int priority = post.getPriority();
        int containersNumber = post.getNumberOfContainers();
        int cores = post.getCores();

        LOG.info(
            "Configuration containers [cores:{}, memory:{}, priority:{}, containers:{}]",
            cores, memory, priority, containersNumber);

        CustomApplicationMaster master =
            context.getBean(CustomApplicationMaster.class);
        if (bindingResult.hasErrors()) {
            LOG.error("Form has errors");
            return "settings";
        }
        model.addAttribute("settings", post);
        master.runApplication(memory, priority, cores, containersNumber);
        return "settings";
    }
}